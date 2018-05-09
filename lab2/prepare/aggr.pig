fs -get s3://cloudacl/code/GeoLiteCity.dat

register s3://cloudacl/code/pig-udf-0.0.1-SNAPSHOT.jar
register s3://cloudacl/code/geoip-api-1.3.1.jar

a = LOAD '$INPUT' AS (line:chararray);
b = FOREACH a GENERATE flatten(REGEX_EXTRACT_ALL(line, '(.*?) .*?\\[(.*?)\\].*?&cat=(.*?) .*')) AS (ip:chararray, dt:chararray, cat:chararray);
c = FILTER b BY ip IS NOT null;
d = FOREACH c generate ip, com.example.pig.GetCountry(ip) AS country, ToString(ToDate(dt, 'dd/MMM/yyyy:HH:mm:ss +0000'), 'yyyy-MM-dd HH:00:00') AS dt, cat;
e = FILTER d BY country IS NOT null;
f = GROUP e BY (country, dt, cat);
g = FOREACH f GENERATE flatten(group), COUNT(e);

STORE g INTO '$OUTPUT';
