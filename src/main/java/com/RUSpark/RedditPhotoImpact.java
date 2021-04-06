package com.RUSpark;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Comparator;

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

		JavaPairRDD<Integer, Integer> parseCount = lines.mapToPair(s -> {  
			String[] tempStr = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);	
			System.out.println("STR: " 
						+ tempStr[0] + ", with vals: " 
						+ tempStr[4] + ", " 
						+ tempStr[5] + ", " 
						+ tempStr[6]);
			Integer sum = Integer.valueOf(tempStr[4]) + Integer.valueOf(tempStr[5]) + Integer.valueOf(tempStr[6]);
			return new Tuple2<>(Integer.valueOf(tempStr[0]), sum);
		});

		JavaPairRDD<Integer, Integer> counts = parseCount.reduceByKey((i, j) -> i + j);

		ArrayList<Tuple2<Integer, Integer>> output = new ArrayList<Tuple2<Integer, Integer>>(counts.collect());

		output.sort(Comparator.comparing(t -> t._1()));

		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + " " + tuple._2());
		}

		spark.stop();
	}

}
