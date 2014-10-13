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

class SnowplowIndicatorsJob(args : Args) extends Job(args) {

  import SnowplowSchemas._
  import SnowplowWrapper._

  val inputFile = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA)

  inputFile.read
           .filterByPageViews
           .normalizeDatePrecision
           .groupByNormalizedDate
           .sortAllByNormalizedDate
           .write(Tsv( args("output") + "/dashboard_hits_by_day_hour"))

  inputFile.read
           .filterByPageViews
           .normalizeDatePrecision
           .uniqueVisitors
           .groupByNormalizedDate
           .sortAllByNormalizedDate
           .write(Tsv( args("output") + "/dashboard_visitors_by_day_hour"))

  inputFile.read
           .filterByPageViews
           .filterByFirstVisit
           .normalizeDatePrecision
           .uniqueVisitors
           .groupByNormalizedDate
           .sortAllByNormalizedDate
           .write(Tsv( args("output") + "/dashboard_new_visitors_by_day_hour"))


  inputFile.read
           .filterByPageViews
           .filterByReturningVisit
           .normalizeDatePrecision
           .uniqueVisitors
           .groupByNormalizedDate
           .sortAllByNormalizedDate
           .write(Tsv( args("output") + "/dashboard_returning_visitors_by_day_hour"))

  inputFile.read
           .filterByPageViews
           .normalizeDatePrecision
           .addVisitId
           .uniqueVisits
           .groupByNormalizedDate
           .sortAllByNormalizedDate
           .write(Tsv( args("output") + "/dashboard_visits_by_day_hour"))

  inputFile.read
           .normalizeDatePrecision
           .addVisitId
           .normalizeUserAgent
           .uniqueVisitsAndUserAgents
           .groupByNormalizedDateAndUserAgent
           .write(Tsv( args("output") + "/browser_by_day_hour"))

  //TODO : pages_per_visit_by_day_hour


  inputFile.read
           .normalizeDatePrecision
           .addSection
           .groupByNormalizedDateAndSection
           .write(Tsv( args("output") + "/pages_per_section_by_day_hour"))

}
