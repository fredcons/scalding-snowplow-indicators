## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/fredcons/scalding-snowplow-indicators.git
    $ cd scalding-snowplow-indicators
    $ sbt assembly

The 'fat jar' is now available as:

    target/scalding-snowplow-indicators-0.1.0.jar

## Running on Amazon EMR

Finally, you are ready to run this job using the [Amazon Ruby EMR client] [emr-client]:

    $ elastic-mapreduce --create --name "scalding-snowplow-indicators" \
      --jar s3n://{{JAR_BUCKET}}/scalding-snowplow-indicators-0.1.0.jar \
      --arg com.snowplowanalytics.hadoop.scalding.SnowplowIndicatorsJob \
      --arg --hdfs \
      --arg --input --arg s3n://{{IN_BUCKET}}/hello.txt \
      --arg --output --arg s3n://{{OUT_BUCKET}}/results

Replace `{{JAR_BUCKET}}`, `{{IN_BUCKET}}` and `{{OUT_BUCKET}}` with the appropriate paths.

### Inspect

Once the output has completed, you should see a folder structure like this in your output bucket:

     results
     |
     +- _SUCCESS
     +- part-00000

Download the `part-00000` file and check that it contains:

	goodbye	1
	hello	1
	world	2

## Running on your own Hadoop cluster

If you are trying to run this on a non-Amazon EMR environment, you may need to edit:

    project/BuildSettings.scala

And comment out the Hadoop jar exclusions:

    // "hadoop-core-0.20.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
    // "hadoop-tools-0.20.2.jar" // "

[wordcount]: https://github.com/twitter/scalding/blob/master/README.md
[scalding]: https://github.com/twitter/scalding/
[snowplow]: http://snowplowanalytics.com
[snowplow-hadoop-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-hadoop-enrich
[spark-example-project]: https://github.com/snowplow/spark-example-project
[emr]: http://aws.amazon.com/elasticmapreduce/
[hello-txt]: https://github.com/snowplow/scalding-example-project/raw/master/data/hello.txt
[emr-client]: http://aws.amazon.com/developertools/2264
[elasticity]: https://github.com/rslifka/elasticity
[spark-plug]: https://github.com/ogrodnek/spark-plug
[lemur]: https://github.com/TheClimateCorporation/lemur
[boto]: http://boto.readthedocs.org/en/latest/ref/emr.html
[license]: http://www.apache.org/licenses/LICENSE-2.0
