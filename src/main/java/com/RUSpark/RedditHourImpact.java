package com.RUSpark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession.builder().appName("RedditPhotoImpact").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		SimpleDateFormat sdf = new SimpleDateFormat("H");
		
		JavaPairRDD<Integer, Integer> parseCount = lines.mapToPair(s -> {  
			String[] tempStr = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);	
			Integer hour = Integer.valueOf(sdf.format(new Date(Long.parseLong(tempStr[1].trim()) * 1000L)));
			Integer sum = Integer.valueOf(tempStr[4]) + Integer.valueOf(tempStr[5]) + Integer.valueOf(tempStr[6]);
			return new Tuple2<>(hour, sum);
		});
		
		JavaPairRDD<Integer, Integer> counts = parseCount.reduceByKey((i, j) -> i + j);

		ArrayList<Tuple2<Integer, Integer>> output = new ArrayList<Tuple2<Integer, Integer>>(counts.collect());

		output.sort(Comparator.comparing(t -> t._1()));

		int i = -1;
		for (Tuple2<Integer, Integer> tuple : output) {
			if (tuple._1() == ++i) 
				System.out.println(tuple._1() + " " + tuple._2());
			else 
				System.out.println(i + " 0");
		}

		spark.stop();
	}

}
