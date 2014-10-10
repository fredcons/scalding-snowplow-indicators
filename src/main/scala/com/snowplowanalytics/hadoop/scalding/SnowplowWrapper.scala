package com.snowplowanalytics.hadoop.scalding

import com.twitter.scalding.RichPipe

object SnowplowWrapper {

  implicit def wrapPipe(self: cascading.pipe.Pipe): SnowplowWrapper = new SnowplowWrapper(new RichPipe(self))
  implicit class SnowplowWrapper(val self: RichPipe) extends SnowplowOperations with Serializable

}