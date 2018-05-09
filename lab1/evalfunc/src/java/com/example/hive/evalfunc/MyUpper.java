package com.example.hive.evalfunc;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

// my own HiveUDF
public class MyUpper extends UDF {
	public Text evaluate(Text input) {
		return new Text(input.toString().toUpperCase());
	}
}
