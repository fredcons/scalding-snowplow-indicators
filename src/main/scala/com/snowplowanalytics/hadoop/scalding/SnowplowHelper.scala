package com.snowplowanalytics.hadoop.scalding

/**
 * Created by fred on 10/12/14.
 */
object SnowplowHelper {

  def transformUserAgent(userAgent:String) = {
    userAgent match {
      case null => "Other"
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

  def extractPageSection(pageUrl:String) = {
    if (pageUrl == null) {
      normalizeSection("")
    } else {
      val sections = pageUrl.split("/")
      if (sections.length <= 1) normalizeSection("") else normalizeSection(sections(1))
    }
  }

  def normalizeSection(sectionName: String) = {
    sectionName match {
      case "" => "Home"
      case "gamme-renault" => "VN"
      case "my-renault" => "MyRenault"
      case "esc" => "MyRenault"
      case "vehicules-occasion" => "Occasions"
      case "apres-vente" => "Après-vente"
      case "services" => "Services"
      case "promotions" => "Promotions"
      case "ebrochure" => "E-Brochure"
      case "quoteonline" => "QuoteOnLine"
      case "renault-parc-entreprises" => "Renault Professionnels"
      case "decouvrez-renault" => "Découvrez Renault"
      case _ => "Autres"
    }
  }

  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}
