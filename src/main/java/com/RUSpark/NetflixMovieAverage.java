package com.RUSpark;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Computes the average rating of every movie and prints {@code movieId averageRating}
 * (two decimals) sorted by movie id.
 *
 * <p>Input columns: {@code movie_id, customer_id, rating, date}.
 *
 * <p>The sum and count are carried together in a single {@code aggregateByKey} pass, so the
 * dataset is scanned once and no per-movie lookup table is broadcast to the executors.
 */
public class NetflixMovieAverage {

  private static final int MOVIE_ID = 0;
  private static final int RATING = 2;

  /** @return (movieId, averageRating), sorted by movieId. */
  static JavaPairRDD<Integer, Double> averageRatings(JavaRDD<String> lines) {
    return lines
        .mapToPair(line -> {
          String[] cols = Csv.split(line);
          return new Tuple2<>(Integer.parseInt(cols[MOVIE_ID]), Double.parseDouble(cols[RATING]));
        })
        // accumulator = (sum of ratings, number of ratings)
        .aggregateByKey(
            new Tuple2<>(0.0, 0L),
            (acc, rating) -> new Tuple2<>(acc._1() + rating, acc._2() + 1),
            (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
        .mapValues(acc -> acc._1() / acc._2())
        .sortByKey();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("NetflixMovieAverage").getOrCreate();
    try {
      JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
      List<Tuple2<Integer, Double>> output = averageRatings(lines).collect();
      for (Tuple2<Integer, Double> row : output) {
        System.out.printf("%d %.2f%n", row._1(), row._2());
      }
    } finally {
      spark.stop();
    }
  }
}
