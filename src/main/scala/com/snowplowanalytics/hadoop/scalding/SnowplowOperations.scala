package com.snowplowanalytics.hadoop.scalding

import cascading.pipe.Pipe
import com.twitter.scalding._

trait SnowplowOperations extends FieldConversions {

  def self: RichPipe

  val fmt = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def normalizeDatePrecision : Pipe = self
    .map('collector_tstamp -> 'collector_tstamp_hour) {
      date: String => fmt.parseDateTime(date).toString("yyyy-MM-dd HH:00:00")
    }

  def groupByNormalizedDate : Pipe = self
    .groupBy('collector_tstamp_hour)  { _.size }

  def filterByPageViews : Pipe = self
    .filter('event) { event:String => event == "page_view" }

  def filterByFirstVisit : Pipe = self
    .filter('domain_sessionidx) { domain_sessionidx:Int => domain_sessionidx == 1 }

  def filterByReturningVisit : Pipe = self
    .filter('domain_sessionidx) { domain_sessionidx:Int => domain_sessionidx > 1 }

  def uniqueVisitors : Pipe = self
    .unique('collector_tstamp_hour, 'domain_userid)

}
