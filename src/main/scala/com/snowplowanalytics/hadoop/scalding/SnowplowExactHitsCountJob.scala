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

import com.twitter.algebird.DenseHLL
import com.twitter.scalding._

class SnowplowExactHitsCountJob(args : Args) extends Job(args) {

  import com.snowplowanalytics.hadoop.scalding.SnowplowSchemas._

  val pageHits = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA).filter('event) { event:String => event == "page_view" }

  val exactHits = pageHits.groupAll{ _.size('count) }

  exactHits.write(Tsv( args("output") + "/exact_hits"))

}