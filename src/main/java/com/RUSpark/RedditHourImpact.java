package com.RUSpark;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Computes the total "impact" ({@code upvotes + downvotes + comments}) of all Reddit posts
 * grouped by the hour of day (0-23, US Eastern time) in which they were posted, and prints
 * {@code hour impact} sorted by hour.
 *
 * <p>Input columns: {@code image_id, unix_time, title, total_votes, upvotes, downvotes, comments}.
 */
public class RedditHourImpact {

  private static final ZoneId EASTERN = ZoneId.of("America/New_York");

  private static final int UNIX_TIME = 1;
  private static final int UPVOTES = 4;
  private static final int DOWNVOTES = 5;
  private static final int COMMENTS = 6;

  /** @return (hourOfDay, impact) summed across all rows for that hour, sorted by hour. */
  static JavaPairRDD<Integer, Long> impactByHour(JavaRDD<String> lines) {
    return lines
        .mapToPair(line -> {
          String[] cols = Csv.split(line);
          long impact = Long.parseLong(cols[UPVOTES])
              + Long.parseLong(cols[DOWNVOTES])
              + Long.parseLong(cols[COMMENTS]);
          return new Tuple2<>(hourOfDay(Long.parseLong(cols[UNIX_TIME])), impact);
        })
        .reduceByKey(Long::sum)
        .sortByKey();
  }

  /** Converts epoch seconds to the hour of day (0-23) in US Eastern time, DST-aware. */
  static int hourOfDay(long epochSeconds) {
    return Instant.ofEpochSecond(epochSeconds).atZone(EASTERN).getHour();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("RedditHourImpact").getOrCreate();
    try {
      JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
      List<Tuple2<Integer, Long>> output = impactByHour(lines).collect();
      for (Tuple2<Integer, Long> row : output) {
        System.out.println(row._1() + " " + row._2());
      }
    } finally {
      spark.stop();
    }
  }
}
