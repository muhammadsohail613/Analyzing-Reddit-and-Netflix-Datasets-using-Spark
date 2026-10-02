package com.RUSpark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Builds a weighted customer-similarity graph. Two customers are connected when they gave the
 * <em>same rating to the same movie</em>; the edge weight is the number of such movies.
 *
 * <p>Prints one edge per line as {@code (customerA,customerB) weight} with
 * {@code customerA < customerB}, so every undirected edge appears exactly once. An optional
 * second argument sets a minimum weight (default 1) to keep the output manageable on large inputs.
 *
 * <p>Input columns: {@code movie_id, customer_id, rating, date}.
 *
 * <p>Note: a movie/rating bucket with <i>n</i> customers produces <i>n(n-1)/2</i> pairs, so very
 * popular buckets dominate the cost of this job.
 */
public class NetflixGraphGenerate {

  private static final int MOVIE_ID = 0;
  private static final int CUSTOMER_ID = 1;
  private static final int RATING = 2;

  /** @return ((customerA, customerB), weight) for every customer pair with weight &gt;= minWeight. */
  static JavaPairRDD<Tuple2<Integer, Integer>, Integer> similarityEdges(
      JavaRDD<String> lines, int minWeight) {
    return lines
        // key = (movie, rating), value = customer
        .mapToPair(line -> {
          String[] cols = Csv.split(line);
          Tuple2<Integer, Integer> movieAndRating =
              new Tuple2<>(Integer.parseInt(cols[MOVIE_ID]), Integer.parseInt(cols[RATING]));
          return new Tuple2<>(movieAndRating, Integer.parseInt(cols[CUSTOMER_ID]));
        })
        .distinct() // a customer re-rating the same movie must not count twice
        .groupByKey()
        // every unordered pair of customers sharing this (movie, rating) bucket
        .flatMapToPair(bucket -> {
          List<Integer> customers = new ArrayList<>();
          bucket._2().forEach(customers::add);
          customers.sort(Integer::compare);

          List<Tuple2<Tuple2<Integer, Integer>, Integer>> pairs = new ArrayList<>();
          for (int i = 0; i < customers.size(); i++) {
            for (int j = i + 1; j < customers.size(); j++) {
              pairs.add(new Tuple2<>(new Tuple2<>(customers.get(i), customers.get(j)), 1));
            }
          }
          return pairs.iterator();
        })
        .reduceByKey(Integer::sum)
        .filter(edge -> edge._2() >= minWeight);
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file> [minWeight]");
      System.exit(1);
    }
    int minWeight = args.length > 1 ? Integer.parseInt(args[1]) : 1;

    SparkSession spark = SparkSession.builder().appName("NetflixGraphGenerate").getOrCreate();
    try {
      JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
      List<Tuple2<Tuple2<Integer, Integer>, Integer>> edges =
          new ArrayList<>(similarityEdges(lines, minWeight).collect());
      edges.sort(Comparator
          .comparing((Tuple2<Tuple2<Integer, Integer>, Integer> e) -> e._1()._1())
          .thenComparing(e -> e._1()._2()));
      for (Tuple2<Tuple2<Integer, Integer>, Integer> edge : edges) {
        System.out.println("(" + edge._1()._1() + "," + edge._1()._2() + ") " + edge._2());
      }
    } finally {
      spark.stop();
    }
  }
}
