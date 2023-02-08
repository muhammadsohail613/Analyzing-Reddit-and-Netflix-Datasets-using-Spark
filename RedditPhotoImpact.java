package com.RUSpark;


import scala.Tuple2;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;




import java.util.regex.Pattern;
import java.util.List;



/* any necessary Java packages here */

public class RedditPhotoImpact {
  private static final Pattern SPACE = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
  
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		/* Implement Here */ 
		// Create SparkSession here
		SparkSession spark = SparkSession
      .builder()
      .appName("RedditPhotoImpact")
      .getOrCreate();

 
     
    // Get lines from the Reddit Data 
    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
    JavaPairRDD<Integer, Integer> temp = lines.mapToPair(s -> {
      String [] cols = SPACE.split(s); 
      int id = Integer.parseInt(cols[0]);
      int upvote =  Integer.parseInt(cols[4]);
      int downvote =  Integer.parseInt(cols[5]);
      int comments =  Integer.parseInt(cols[6]);

      return new Tuple2<>(id, upvote + downvote + comments); 
    });   

    JavaPairRDD<Integer, Integer> counts = temp.reduceByKey((i1, i2) -> i1 + i2);
    JavaPairRDD<Integer, Integer> res = counts.sortByKey();
    List<Tuple2<Integer, Integer>> output = res.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + " " + tuple._2());
    }
    
    spark.stop();
  
	}

}
