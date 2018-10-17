package org.avanhecken.tpcds.query

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigValue}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class QueryFactoryTest extends FlatSpec with Matchers {
  "queries" should "be the full list of all queries to benchmark" in {
    val queries = new QueryFactory(Map("resource_location" -> getClass.getResource("/").getPath)).generateQueries("test")

    queries.size shouldBe 2
  }
}
