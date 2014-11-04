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

class SnowplowRecommenderJob(args : Args) extends Job(args) {

  import com.snowplowanalytics.hadoop.scalding.SnowplowSchemas._
  import com.snowplowanalytics.hadoop.scalding.SnowplowWrapper._

  val hitPairScheme = ('page_urlpath, 'page_urlpath2, 'jaccardSimilarity, 'size, 'numHits, 'numHits2)
  type HitPair = (String, String, Double, Double, Double, Double)
  def moreSimilar(hitpair1: HitPair, hitpair2: HitPair) = hitpair1._3 > hitpair2._3


  val pageHits = Tsv(args("input"), SNOWPLOW_INPUT_SCHEMA).filter('event) { event:String => event == "page_view" }

  // Put the number of people who viewed the page into a field called "numHits".
  val numHits = pageHits.groupBy('page_urlpath) { _.size }.rename('size -> 'numHits)

  val hitsWithSize = pageHits.joinWithSmaller('page_urlpath -> 'page_urlpath, numHits)
                             .unique('domain_userid, 'page_urlpath, 'numHits)

  val pageHits2 = hitsWithSize.rename(('domain_userid, 'page_urlpath, 'numHits) -> ('domain_userid2, 'page_urlpath2, 'numHits2))
                              .project('domain_userid2, 'page_urlpath2, 'numHits2)

  val hitPairs = hitsWithSize.joinWithSmaller('domain_userid -> 'domain_userid2, pageHits2)
                             .filter('page_urlpath, 'page_urlpath2) { urls : (String, String) => urls._1 != urls._2 }
                             .filter('page_urlpath, 'page_urlpath2) { urls : (String, String) => !(urls._1 contains urls._2) }
                             .filter('page_urlpath, 'page_urlpath2) { urls : (String, String) => urls._1 != "/" && urls._2 != "/" }
                             .project('page_urlpath, 'numHits, 'page_urlpath2, 'numHits2)

  val vectorCalcs = hitPairs.groupBy('page_urlpath, 'page_urlpath2) { group =>
        group.size // length of each vector
             .max('numHits) // Just an easy way to make sure the numHits field stays.
             .max('numHits2)
  }

  val similarities = vectorCalcs.map(('size, 'numHits, 'numHits2) -> 'jaccardSimilarity) {
    fields : (Double, Double, Double) =>
    val (size, numHits, numHits2) = fields
    SnowplowHelper.jaccardSimilarity(size, numHits, numHits2)
  }

  val topSimilarities = similarities.groupBy('page_urlpath) {_.sortWithTake(hitPairScheme  -> 'mostSimilarPages, 5) (moreSimilar)}
                                    .flattenTo[HitPair]('mostSimilarPages -> hitPairScheme)

  topSimilarities.write(Tsv( args("output") + "/top_page_similarities.tsv"))

}