package com.RUSpark;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: NetflixMovieAverage <file>");
			System.exit(1);
		}

		String InputPath = args[0];

		/* Implement Here */ 
		SparkSession spark = SparkSession.builder().appName("NetflixMovieAverage").getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaPairRDD<Integer, Tuple2<Double, Integer>> parseCount = lines.mapToPair(s -> {  
			String[] tempStr = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);	
			return new Tuple2<>(Integer.valueOf(tempStr[0]), new Tuple2<Double, Integer>(Double.valueOf(tempStr[2]), 1));
		});

		JavaPairRDD<Integer, Tuple2<Double, Integer>> counts = parseCount.reduceByKey((i, j) -> new Tuple2<Double, Integer>(i._1() + j._1(), i._2() + j._2()));

		JavaPairRDD<Integer, Double> avgs = counts.mapToPair(i -> new Tuple2<>(i._1(), i._2()._1() / i._2()._2().doubleValue()));

		ArrayList<Tuple2<Integer, Double>> output = new ArrayList<Tuple2<Integer, Double>>(avgs.collect());

		output.sort(Comparator.comparing((Tuple2<Integer, Double> t) -> t._1()));

		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + " " + String.format("%.2f", tuple._2()));
		}

		spark.stop();
	}

}
