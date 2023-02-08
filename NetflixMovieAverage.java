package com.RUSpark;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {
  private static final Pattern COMMA = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

	public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession 
      .builder()
      .appName("NetflixMovieAverage") 
      .getOrCreate(); 

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD(); 
    
    JavaPairRDD<Integer, Float> temp = lines.mapToPair(s -> {

      String [] cols = COMMA.split(s);
      
      int movieID = Integer.parseInt(cols[0]); 
    // String movieID = cols[0]; 
      float rating = Float.parseFloat(cols[2]);//.parseInt(cols[2]); 

      

      return new Tuple2<Integer, Float>(movieID, rating); 
    });

    //JavaPairRDD<Integer, Iterable<String>> rddY = lines.groupBy(s -> Character.getNumericValue(s.charAt(0)));
    JavaPairRDD<Integer, Float> cleanSlate = lines.mapToPair(s -> {

      String [] cols = COMMA.split(s); 

      int movieID = Integer.parseInt(cols[0]); 


      return new Tuple2<Integer, Float>(movieID, 1.0f); 
  });
  // Has the total count the movieid appears
  JavaPairRDD<Integer, Float> counter = cleanSlate.reduceByKey((i1, i2) -> i1 + 1.0f);
    
   
    JavaPairRDD<Integer, Float> counts = temp.reduceByKey((i1, i2) -> i1 + i2); 
    // RDD data now looks like <movieID, Σ Rating>

  
      List<Tuple2<Integer, Float>> totalOccurences = counter.collect();



     JavaPairRDD<Integer, Float> finalOutput = counts.mapToPair(s -> {
        
      int movieID = s._1(); 
      Float averageRating = 0.0f; 
     // System.out.println("movieID: " + movieID); 
     // System.out.println("numberOfOccurrences: " + s._2()); 
      
      for(Tuple2<?,?> tuple : totalOccurences) {
        if ( Integer.parseInt(tuple._1().toString()) == movieID ) {
   
        averageRating = s._2() / Float.parseFloat(tuple._2().toString()); 
     
          break;
        }
   
      }

      return new Tuple2<Integer, Float>(movieID, averageRating);
     });


  JavaPairRDD<Integer, Float> res = finalOutput.sortByKey(); 
  List<Tuple2<Integer, Float>> output = res.collect(); 
 // System.out.println(counter.collect()); 
  for(Tuple2<?,?> tuple : output) {
    System.out.printf("%d %.02f\n", tuple._1(), Float.parseFloat(tuple._2().toString()));
    //System.out.println(tuple._1() + " " + tuple._2()); 
  }
 
  }
}
