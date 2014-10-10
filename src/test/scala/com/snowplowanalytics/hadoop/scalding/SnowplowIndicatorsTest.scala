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

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

class SnowplowIndicatorsTest extends Specification {

  import SnowplowSchemas._
  /*
  "A snowplow indicators job" should {
    JobTest("com.snowplowanalytics.hadoop.scalding.SnowplowIndicatorsJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(Tsv("inputFile"), List(("","web","2014-09-02 03:34:12.532","2014-09-02 00:47:42.000","2014-09-02 00:47:20.288","struct","950ded44-b246-4f5a-93fc-9589751f19af","603013","cf","js-2.0.0","cloudfront","hadoop-0.6.0-common-0.5.0","","88.170.210.x","4092229060","df0a4eb2523fc689","2","","FR","A8","Paris","","48.8667","2.3332977","Ile-de-France","","","","","http://www.renault.fr/gamme-renault/vehicules-particuliers/twingo/twingo/configurateur/","","http://www.tf1.fr/auto-moto/actualite/renault-twingo-2014-prix-a-partir-de-10-800-euros-8478041.html","http","www.renault.fr","80","/gamme-renault/vehicules-particuliers/twingo/twingo/configurateur/","","","http","www.tf1.fr","80","/auto-moto/actualite/renault-twingo-2014-prix-a-partir-de-10-800-euros-8478041.html","","","unknown","","","","","","","","","Discovered | Navigation","Navigation buttons","Top | OPTIONS & ACCESSOIRES","","","","","","","","","","","","","","","","","","","","","","Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36","Chrome","Chrome","36.0.1985.143","Browser","WEBKIT","fr","1","1","1","0","0","0","1","0","1","1","24","1680","925","Windows","Windows","Microsoft Corporation","Europe/Berlin","Computer","0","1680","1050","UTF-8","1663","928"))).
      sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "count by country correctly" in {
          outMap("FR") must be_==(1)
        }
      }.
      run.
      finish
  }*/
}