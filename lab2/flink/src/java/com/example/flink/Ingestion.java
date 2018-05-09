package com.example.flink;

import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class Ingestion {
    static final String hdfsRoot = "/user/hive/warehouse/access_log_partitioned/";
    static SimpleDateFormat inputFormatter = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
    static SimpleDateFormat partitionFormatter = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat timeFormatter = new SimpleDateFormat("HH");
    public static class HDFSSink extends RichSinkFunction<Tuple2<Tuple3<String, String, String>, Integer>> {
        Logger LOG = LoggerFactory.getLogger(HDFSSink.class);
        FileSystem fs;
        public void open(Configuration conf) throws IOException {
            fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        }
        @Override
        public void invoke(Tuple2<Tuple3<String, String, String>, Integer> value) throws Exception {
            LOG.info("writing {}", value);
            Date d = inputFormatter.parse(value.f0.f1);
            String partition = "d=" + partitionFormatter.format(d);
            String time = timeFormatter.format(d);
            fs.mkdirs(new Path(hdfsRoot + partition));
            FSDataOutputStream stream = fs.create(new Path(hdfsRoot + partition, value.f0.f0 + "_" + time + "_" + value.f0.f2));
            PrintStream ps = new PrintStream(stream);
            ps.println(value.f0.f0 + "\t" + value.f0.f1 + "\t" + value.f0.f2 + "\t" + value.f1);
            ps.close();
        }
    }

    public static class ExtractFields extends RichMapFunction<String, Tuple3<String, String, String>> {
        final Pattern linePattern = Pattern.compile("(.*?) .*?\\[(.*?)\\].*?&cat=(.*?) .*");
        static LookupService cl;
        static Object lock = new Object();
        @Override
        public void open(Configuration conf) throws IOException {
            synchronized(lock) {
                if (cl == null) {
                    String dataFileName = getRuntimeContext().getDistributedCache().getFile("GeoLiteCity.dat").getAbsolutePath();
                    cl = new LookupService(dataFileName,
                            LookupService.GEOIP_MEMORY_CACHE );
                }
            }
        }
        @Override
        public Tuple3<String, String, String> map(String line) throws Exception {
            Matcher m = linePattern.matcher(line);
            String ip = null;
            String dt = null;
            String cat = null;
            if (m.find()) {
                ip = m.group(1);
                dt = m.group(2);
                cat = m.group(3);
            }
            Location loc = cl.getLocation(ip);
            return new Tuple3<String, String, String>(loc==null?null:loc.countryCode, dt, cat);
        }
    }

    static class AccessLogTimestampsGenerator implements TimestampExtractor<Tuple3<String, String, String>> {
        private long ct;
        static SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0000");
        @Override
        public long extractTimestamp(Tuple3<String, String, String> element, long previousElementTimestamp) {
            String dt = element.f1;
            try {
                Date d = formatter.parse(dt);
                ct = d.getTime();
            } catch (ParseException e) {
                // Shall not happen
                throw new RuntimeException(e);
            }
            return ct; // tell flink what is Timestamp and it can only read long type of data
        }
        @Override
        public long extractWatermark(Tuple3<String, String, String> element, long currentTimestamp) {
            return currentTimestamp;
        }
        @Override
        public long getCurrentWatermark() {
            return ct;
        }        
    }

    static class ExtractKeyValue extends RichMapFunction<Tuple3<String, String, String>, Tuple2<Tuple3<String, String, String>, Integer>> {
        static SimpleDateFormat incomeFormatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0000");
        static SimpleDateFormat outgoingFormatter = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
        @Override
        public Tuple2<Tuple3<String, String, String>, Integer> map(Tuple3<String, String, String> value) throws Exception {
            Date d = incomeFormatter.parse(value.f1);
            String dt = outgoingFormatter.format(d);
            Tuple3<String, String, String> key = new Tuple3<String, String, String>(value.f0, dt, value.f2);
            return new Tuple2<Tuple3<String, String, String>, Integer>(key, 1);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.registerCachedFile("file:///src/week3/lab2/flink/resource/GeoLiteCity.dat", "GeoLiteCity.dat");
        env.setParallelism(2);

        String path = "file:///mnt/access_log";
        DataStream<Tuple2<Tuple3<String,String, String>,Integer>> dataStream = env
                .readFileStream(path, Time.seconds(1).toMilliseconds(), WatchType.PROCESS_ONLY_APPENDED)
                .map(new ExtractFields())
                .filter(t->t.f0!=null) /* filter null out*/
                .assignTimestamps(new AccessLogTimestampsGenerator()) /*extract timestamps*/
                .map(new ExtractKeyValue())
                .keyBy(0) /* shuffle based on country, assign to different processor*/
                .window(TumblingEventTimeWindows.of(Time.hours(1))) /* microbatch aggregation job. Window size is one hour. */
                .sum(1); /* sum on 1st field*/

        dataStream.addSink(new HDFSSink()); // data sink function. Write file into HDFS

        env.execute("Hourly data ingestion");
    }
}
