package com.sample.Work;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Sample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
	    DataFrame df = sqlContext.read().json("B:\\Data\\SSS.json");
	    
	    df.show();
	    
	    df.toJavaRDD().map(new Function<Row,String>(){

			public String call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.toString();
			}
	    	
	    }).foreach(new VoidFunction<String>(){

			public void call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
	    
	    });;
	}

}
