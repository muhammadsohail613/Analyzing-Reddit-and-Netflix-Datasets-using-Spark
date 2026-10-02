#!/usr/bin/env bash
# Run one of the Spark jobs locally with spark-submit.
#
#   scripts/run.sh <JobName> <input.csv> [extra job args]
#
# Example:
#   scripts/run.sh NetflixMovieAverage data/sample/netflix_sample.csv
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <RedditPhotoImpact|RedditHourImpact|NetflixMovieAverage|NetflixGraphGenerate> <input.csv> [args...]" >&2
  exit 1
fi

JOB="$1"; shift
cd "$(dirname "$0")/.."

mvn -q -B package -DskipTests
spark-submit \
  --class "com.RUSpark.${JOB}" \
  --master "${SPARK_MASTER:-local[*]}" \
  target/spark-reddit-netflix-analytics-1.0.0.jar "$@"
