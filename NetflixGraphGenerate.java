package com.RUSpark;


import java.util.List;

import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


import org.apache.spark.sql.SparkSession;



import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixGraphGenerate {
  private static final Pattern COMMA = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
    SparkSession spark = SparkSession 
    .builder()
    .appName("NetflixMovieAverage") 
    .getOrCreate(); 

    // Read in file
    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD(); 
    // Parse the movieID, ratings and make new tuple
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> temp = lines.mapToPair(s -> {

      String [] cols = COMMA.split(s);
      
      int movieID = Integer.parseInt(cols[0]);
      int customerID = Integer.parseInt(cols[1]); 
      // String movieID = cols[0]; 
      int rating = Integer.parseInt(cols[2]);
     // Tuple2<Integer, Integer> movieRatingPair = new Tuple2<>(movieID, rating); 
     Tuple2<Integer, Integer> sup = new Tuple2<>(movieID, rating);
    //  Tuple2<Tuple2<Integer, Integer>, Integer> sup = new Tuple2<>(movieRatingPair, customerID);
      
      
      return new Tuple2<Tuple2<Integer, Integer>, Integer>(sup, customerID); 
    });

    
   // temp now holds the tuple like: (<movieID, rating>, 1) 
    List<Tuple2<Tuple2<Integer, Integer>, Integer>> totalOccurences = temp.collect();
    
   // List<Tuple2<Tuple2<Integer, Integer>, Integer>> temporary = cleanSlate.collect();

    JavaPairRDD<Tuple2<Integer, Integer>, Integer> tempTwo = temp.mapToPair(s -> {
      int customeridOne = 0; 
      int customeridTwo = 0; 
      Tuple2<Integer, Integer> sup =  new Tuple2<>(0,0); 
      for(Tuple2<Tuple2<Integer, Integer>, Integer> tuple : totalOccurences ) {
        customeridOne = tuple._2();
        customeridTwo = s._2(); 

        if ( customeridOne < customeridTwo ) {
          sup = new Tuple2<>(customeridOne, customeridTwo);
        }

      }

      return new Tuple2<Tuple2<Integer, Integer>, Integer>(sup, 1);
    });

    JavaPairRDD<Tuple2<Integer, Integer>, Integer> tempThree = tempTwo.reduceByKey((i1,i2) -> i1 + i2);

  
    
    List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = tempThree.collect(); 

    for(Tuple2<Tuple2<Integer, Integer>, Integer> tuple : output) {

      if ( tuple._2() > 1 ) {
        System.out.println("(" + tuple._1()._1() + "," + tuple._1()._2() + ")" + " " +  tuple._2() );

      }
       
    }


	}

}
