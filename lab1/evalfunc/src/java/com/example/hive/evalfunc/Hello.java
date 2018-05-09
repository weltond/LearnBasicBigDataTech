package com.example.hive.evalfunc;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Hello extends UDF {
    public Text evaluate(Text input) {
        return new Text("Hello " + input.toString());
    }
}
