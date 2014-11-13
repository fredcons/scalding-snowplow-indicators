/*
 * Copyright (c) 2012 Twitter, Inc.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.hadoop.scalding

// Scalding

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Histogram

class SnowplowScreenWidthHistogramJob(args : Args) extends Job(args) {

  import com.snowplowanalytics.hadoop.scalding.SnowplowSchemas._

  val pageHits = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA).filter('event) { event:String => event == "page_view" }

  // screenwidth is not that interesting, but it's one of the few numeric values around
  pageHits.groupAll { group => group.histogram(('br_viewwidth, 'histogram)) }
    .mapTo('histogram -> ('q1, 'median, 'q3, 'innerQuartileRange, 'coefficientOfDispersion, 'mean, 'ninetyfive, 'ninetynine, 'max, 'min, 'stddev)) {
    x:Histogram => (x.q1 , x.median , x.q3, x.innerQuartileRange, x.coefficientOfDispersion, x.mean, x.percentile(95), x.percentile(99), x.max, x.min, x.stdDev)
  }.write(Tsv( args("output") + "/screen_width_histogram"))


}