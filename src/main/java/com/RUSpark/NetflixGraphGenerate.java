package com.RUSpark;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession.builder().appName("NetflixMovieAverage").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> pairs = lines.mapToPair(s -> {
			String[] tempStr = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			return new Tuple2<>(new Tuple2<Integer, Integer>(Integer.valueOf(tempStr[0]), Integer.valueOf(tempStr[2])), Integer.valueOf(tempStr[1]));
		});
		
		JavaPairRDD<Tuple2<Tuple2<Integer,Integer>,Integer>,Tuple2<Tuple2<Integer,Integer>,Integer>> pairs2 = pairs.cartesian(pairs)
				.filter((Tuple2<Tuple2<Tuple2<Integer,Integer>,Integer>,Tuple2<Tuple2<Integer,Integer>,Integer>> a) -> a._1()._2() < a._2()._2());
		
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> ones = pairs2.mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1()._2(), p._2()._2()), 1));
		
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> counts = ones.reduceByKey((i,j) -> i + j);
		
		ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>(counts.collect());
		
		output.sort(Comparator.comparing(t -> t._1()._1()));
		output.sort(Comparator.comparing(t -> t._1()._2()));
				
		for (Tuple2<Tuple2<Integer, Integer>, Integer> t : output) {
			System.out.println(t._1()._1() + " " + t._1()._2() + " " + t._2());
		}
		
	}

}