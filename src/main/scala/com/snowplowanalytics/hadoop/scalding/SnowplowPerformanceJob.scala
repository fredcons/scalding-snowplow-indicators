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
import scala.util.parsing.json._
import com.twitter.scalding.mathematics.Histogram

class SnowplowPerformanceJob(args : Args) extends Job(args) {

  import com.snowplowanalytics.hadoop.scalding.SnowplowSchemas._

  case class PerformanceEntry(resource: String, duration: Double )

  val pageHits = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA).filter('event) { event:String => event == "page_view" }

  val performanceEntries = pageHits.mapTo('contexts -> ('parse_status, 'performanceEntriesAsJson)) {
    contexts: String =>  {
      JSON.parseFull(contexts) match {
        case Some(data: Map[String, Any]) => {
          val entries = data("data").asInstanceOf[List[Map[String, Map[String, List[Map[String, Any]]]]]](2)("data")("entrie")
          ("success", entries)
        }
        case None => ("failed", "")
      }
    }
  }
  // remove invalid lines
  .filter('parse_status) { status: String => status != "failed" }
  .discard('parse_status)
  // emit one tuple per list element
  .flatMapTo('performanceEntriesAsJson -> 'performanceEntries) { entries : List[Map[String, Any]] => entries }
  // keep only name and duration
  .mapTo('performanceEntries -> ('name, 'duration)) { entry : Map[String, Any] => (entry("name").asInstanceOf[String], entry("duration").asInstanceOf[Double]) }
  // group by name and compute histogram
  .groupBy('name) { group => group.histogram(('duration, 'histogram)) }
  .map('histogram -> ('min, 'max, 'mean, 'median, 'stddev, 'coefficientOfDispersion, 'innerQuartileRange, 'q1, 'q3, 'ninetyfive, 'ninetynine)) {
    h: Histogram => (h.min, h.max, h.mean, h.median, h.stdDev, h.coefficientOfDispersion, h.innerQuartileRange, h.q1, h.q3, h.percentile(95), h.percentile(99))
  }
  .discard('histogram)
  .write(Tsv( args("output") + "/performance_entries"))

}