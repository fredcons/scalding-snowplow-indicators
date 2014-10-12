package com.snowplowanalytics.hadoop.scalding

/**
 * Created by fred on 10/12/14.
 */
object SnowplowHelper {

  def transformUserAgent(userAgent:String) = {
    userAgent match {
      case ua if ua.contains("iPad") => "Tablet"
      case ua if ua.contains("iPhone") => "Phone"
      case ua if ua.contains("Android") => "Phone"
      case ua if ua.contains("Nokia") => "Phone"
      case ua if ua.contains("Windows Phone") => "Phone"
      case ua if ua.contains("BlackBerry") => "Phone"
      case ua if ua.contains("Mobile") => "Phone"
      case ua if ua.contains("Opera Mini") => "Phone"
      case ua if ua.contains("Windows") => "Desktop"
      case ua if ua.contains("W11") => "Desktop"
      case ua if ua.contains("Macintosh") => "Desktop"
      case ua if ua.contains("PLAYSTATION") => "Other"
      case _ => "Other"
    }
  }

}
