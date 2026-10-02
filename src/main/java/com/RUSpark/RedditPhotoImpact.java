package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Computes the total "impact" of every Reddit image, where
 * {@code impact = upvotes + downvotes + comments}, and prints {@code imageId impact}
 * sorted by image id.
 *
 * <p>Input columns: {@code image_id, unix_time, title, total_votes, upvotes, downvotes, comments}.
 */
public class RedditPhotoImpact {

  private static final int ID = 0;
  private static final int UPVOTES = 4;
  private static final int DOWNVOTES = 5;
  private static final int COMMENTS = 6;

  /** @return (imageId, impact) summed across all rows for that image, sorted by imageId. */
  static JavaPairRDD<Integer, Long> impactByPhoto(JavaRDD<String> lines) {
    return lines
        .mapToPair(line -> {
          String[] cols = Csv.split(line);
          long impact = Long.parseLong(cols[UPVOTES])
              + Long.parseLong(cols[DOWNVOTES])
              + Long.parseLong(cols[COMMENTS]);
          return new Tuple2<>(Integer.parseInt(cols[ID]), impact);
        })
        .reduceByKey(Long::sum)
        .sortByKey();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("RedditPhotoImpact").getOrCreate();
    try {
      JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
      List<Tuple2<Integer, Long>> output = impactByPhoto(lines).collect();
      for (Tuple2<Integer, Long> row : output) {
        System.out.println(row._1() + " " + row._2());
      }
    } finally {
      spark.stop();
    }
  }
}
