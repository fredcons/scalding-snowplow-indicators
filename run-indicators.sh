#!/bin/bash
java -cp target/scala-2.10/scalding-snowplow-indicators-0.1.0.jar com.twitter.scalding.Tool com.snowplowanalytics.hadoop.scalding.SnowplowIndicatorsJob --local --input data/enriched-archives.txt  --output data/results/
