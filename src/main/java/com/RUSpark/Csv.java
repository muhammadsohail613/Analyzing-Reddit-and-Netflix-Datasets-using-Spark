package com.RUSpark;

import java.util.regex.Pattern;

/** Minimal CSV helper shared by all jobs. */
final class Csv {

  /** Splits on commas that are not enclosed in double quotes (e.g. post titles containing commas). */
  private static final Pattern COMMA = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

  private Csv() {}

  static String[] split(String line) {
    return COMMA.split(line);
  }
}
