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
import com.twitter.scalding.mathematics.Matrix

class SnowplowMatrixJob(args : Args) extends Job(args) {

  import com.snowplowanalytics.hadoop.scalding.SnowplowSchemas._
  import Matrix._

  val pageHits = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA).filter('event) { event:String => event == "page_view" }

  val pagesAndUsersWithCounts = pageHits.groupBy(('page_urlpath, 'domain_userid)) { _.size }

  val pagesAndUsersMatrix = pagesAndUsersWithCounts.toMatrix[String,String,Double]('page_urlpath, 'domain_userid, 'size)

  val normMatrix = pagesAndUsersMatrix.rowL2Normalize

  // we compute the innerproduct of the normalized matrix with itself
  // which is equivalent with computing cosine: AA^T / ||A|| * ||A||
  (normMatrix * normMatrix.transpose).write(Tsv( args("output") + "/pages_cosine_similarity"))

}