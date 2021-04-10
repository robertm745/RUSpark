package com.RUSpark;

import java.util.ArrayList;
import java.util.Collections;
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
		SparkSession spark = SparkSession.builder().appName("NetflixGraphGenerate").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		// map each line to a ((movieID, ratingVal), custID) tuple, i.e. Tuple2<Tuple2<Movie, Rating>, Customer> 
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> pairs = lines
				.mapToPair(s -> {
					String[] tempStr = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1); 
					return new Tuple2<>(
							new Tuple2<Integer, Integer>(
									Integer.valueOf(tempStr[0]), // movie id
									Integer.valueOf(tempStr[2]) // rating 
							), 
							Integer.valueOf(tempStr[1]) // customer id
						);
				});
		
		// perform cross product to extract all unique tuples of (((movieID, ratingVal), custID_1), ((movieID, ratingVal), custID_2)) 
		JavaPairRDD<Tuple2<Tuple2<Integer,Integer>,Integer>,Tuple2<Tuple2<Integer,Integer>,Integer>> pairs2 = pairs
				.cartesian(pairs)
				.filter(
						// extract only tuples with custID_1 < custID_2 to avoid double counting each connection
						(Tuple2<Tuple2<Tuple2<Integer,Integer>,Integer>,Tuple2<Tuple2<Integer,Integer>,Integer>> a) -> 
							a._1()._1()._1() == a._2()._1()._1()	 	// equal movieID
							&& a._1()._1()._2() == a._2()._1()._2()	 	// equal ratingVal
							&& a._1()._2() < a._2()._2()			 	// custID_1 < custID_2
				);
		
		// map to ((custID_1, custID_2), connection_count)
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> ones = 
				pairs2
				.mapToPair(p -> new Tuple2<>(
												new Tuple2<>(
														p._1()._2(),
														p._2()._2()
												),
												1
											)
						);
			
		// sum connection counts by key
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> counts = ones.reduceByKey((i,j) -> i + j);
		
		// collect
		ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>(counts.collect());
		
		// customized sort
		output.sort(
					Comparator.comparing((Tuple2<Tuple2<Integer, Integer>, Integer> t) -> t._2()) // sort first by weight of connection
					.reversed() // descending order
					.thenComparing(t -> t._1()._1()) // then sort by customerID_1
					.thenComparing(t -> t._1()._2()) // then by customerID_2
			);
		
		for (Tuple2<Tuple2<Integer, Integer>, Integer> t : output) {
			System.out.println(t._1()._1() + " " + t._1()._2() + " " + t._2());
		}
		
	}

}