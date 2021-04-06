package com.RUSpark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession.builder().appName("RedditPhotoImpact").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		JavaPairRDD<String, Integer> parseCount = lines.mapToPair(s -> {  
																String[] tempStr = s.split(",");
																Integer sum = Integer.parseInt(tempStr[4]) + Integer.parseInt(tempStr[5]) + Integer.parseInt(tempStr[6]);
																return new Tuple2<>(tempStr[0], sum);
															});
		JavaPairRDD<String, Integer> counts = parseCount.reduceByKey((i, j) -> i + j);
	    List<Tuple2<String, Integer>> output = counts.collect();
	    for (Tuple2<?,?> tuple : output) {
	        System.out.println(tuple._1() + " " + tuple._2());
	      }
	  		
	      spark.stop();
	}

}
