package com.RUSpark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

/** End-to-end checks of each job against the small sample datasets in {@code data/sample}. */
class SparkJobsTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("SparkJobsTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
  }

  @AfterAll
  static void stopSpark() {
    spark.stop();
  }

  private static JavaRDD<String> read(String file) {
    return spark.read().textFile("data/sample/" + file).javaRDD();
  }

  private static <K, V> Map<K, V> toMap(List<Tuple2<K, V>> pairs) {
    Map<K, V> map = new LinkedHashMap<>();
    pairs.forEach(p -> map.put(p._1(), p._2()));
    return map;
  }

  @Test
  void photoImpactSumsVotesAndCommentsPerImage() {
    Map<Integer, Long> result = toMap(RedditPhotoImpact.impactByPhoto(read("reddit_sample.csv")).collect());

    // image 1: (80+20+5) + (10+5+1); image 2: (30+10+2) + (4+1+1); image 3: 5+1+0
    assertEquals(Map.of(1, 121L, 2, 48L, 3, 6L), result);
    assertEquals(List.of(1, 2, 3), List.copyOf(result.keySet()), "sorted by image id");
  }

  @Test
  void hourImpactGroupsByEasternHourIncludingDaylightSaving() {
    Map<Integer, Long> result = toMap(RedditHourImpact.impactByHour(read("reddit_sample.csv")).collect());

    // 17:13 EST -> 105 + 42, 18:13 EST -> 16, 07:06 EST -> 6, 00:26 EDT (summer) -> 6
    assertEquals(Map.of(0, 6L, 7, 6L, 17, 147L, 18, 16L), result);
  }

  @Test
  void hourOfDayHonoursTimezone() {
    assertEquals(17, RedditHourImpact.hourOfDay(1700000000L)); // UTC-5 in November
    assertEquals(0, RedditHourImpact.hourOfDay(1690000000L));  // UTC-4 in July
  }

  @Test
  void movieAverageIsMeanRatingPerMovie() {
    Map<Integer, Double> result = toMap(NetflixMovieAverage.averageRatings(read("netflix_sample.csv")).collect());

    assertEquals(3, result.size());
    assertEquals(14.0 / 3, result.get(1), 1e-9);
    assertEquals(5.0, result.get(2), 1e-9);
    assertEquals(10.0 / 3, result.get(3), 1e-9);
  }

  @Test
  void graphConnectsCustomersWithSameRatingOnSameMovie() {
    Map<Tuple2<Integer, Integer>, Integer> result =
        toMap(NetflixGraphGenerate.similarityEdges(read("netflix_sample.csv"), 1).collect());

    // (10,20) agree on movies 1 and 2; (10,30) and (20,30) agree only on movie 2.
    // The duplicated row for customer 10 / movie 3 must not create a self-loop.
    assertEquals(
        Map.of(new Tuple2<>(10, 20), 2, new Tuple2<>(10, 30), 1, new Tuple2<>(20, 30), 1),
        result);
  }

  @Test
  void graphHonoursMinimumWeight() {
    Map<Tuple2<Integer, Integer>, Integer> result =
        toMap(NetflixGraphGenerate.similarityEdges(read("netflix_sample.csv"), 2).collect());

    assertEquals(Map.of(new Tuple2<>(10, 20), 2), result);
  }
}
