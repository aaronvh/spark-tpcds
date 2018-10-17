package org.avanhecken.tpcds.query

import java.io.File

import com.typesafe.config.ConfigFactory

trait QueryConfig {
  val resourceLocation: String

  lazy val config = ConfigFactory.parseFile(new File(s"$resourceLocation/queries/queries.conf"))

  def getBusinessQuestion(key: String): String =
    if (config.hasPath(key))
      config.getString(s"queries.$key.business_question")
    else
      "No business question."

  def getClass(key: String): String =
    if (config.hasPath(key))
      config.getString(s"queries.$key.class")
    else
      "No class."
}
