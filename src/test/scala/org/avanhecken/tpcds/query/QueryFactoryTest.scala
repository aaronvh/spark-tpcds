package org.avanhecken.tpcds.query

import org.scalatest.{FlatSpec, Matchers}

class QueryFactoryTest extends FlatSpec with Matchers {
  "queries" should "be the full list of all queries to benchmark" in {
    val queries = QueryFactory.queries

    queries.size shouldBe 1
  }
}
