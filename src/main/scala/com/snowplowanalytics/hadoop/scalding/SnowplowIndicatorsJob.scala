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

  Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA)
    .read
    .groupBy('geo_country) { _.size }
    .write(Tsv( args("output")))

}