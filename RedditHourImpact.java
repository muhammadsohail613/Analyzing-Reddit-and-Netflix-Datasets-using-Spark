package com.RUSpark;



import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;


import scala.Tuple2;
//import org.apache.spark.sql.functions.{col,hour,minute,second}

/* any necessary Java packages here */
public class RedditHourImpact {
  private static final Pattern COMMA = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

	public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		/* Implement Here */ 
		
    SparkSession spark = SparkSession
    .builder()
    .appName("RedditPhotoImpact")
    .getOrCreate();
		
    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD(); 
    
    JavaPairRDD<Integer, Integer> temp = lines.mapToPair(s ->{
      
      String [] cols = COMMA.split(s); 
      
      int hour = Hour(Integer.parseInt(cols[1]));  //Integer.parseInt(Hour((Integer.parseInt(cols[1])))); 
      int upvote =  Integer.parseInt(cols[4]);
      int downvote =  Integer.parseInt(cols[5]);
      int comments =  Integer.parseInt(cols[6]);
      
      return new Tuple2<>(hour, upvote + downvote + comments); 
    });
    
    JavaPairRDD<Integer, Integer> counts = temp.reduceByKey((i1, i2) -> i1 + i2); 
    JavaPairRDD<Integer, Integer> res = counts.sortByKey(); 
    List<Tuple2<Integer, Integer>> output = res.collect(); 
    
    for(Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + " " + tuple._2()); 
    }
    
	}
  
  private static int Hour(int parseInt) {
    long unixSeconds =  parseInt; 
    Date date = new java.util.Date(unixSeconds*1000L); 
    SimpleDateFormat sdf = new java.text.SimpleDateFormat("H"); 
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("America/New_York"));
    String formattedHour = sdf.format(date); 

    return Integer.parseInt(formattedHour); 
  }




}
