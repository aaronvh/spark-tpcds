package org.avanhecken.tpcds.query

import java.io.File

import query._

import scala.org.avanhecken.tpcds.ArgumentParser.Args

/**
  * Factory to create the 99 queries of the TPC-DS benchmark.
  */
object QueryFactory {
  final val queries: Map[Short, Query] = Map(
    makeQuery(
      1,
      """Find customers who have returned items more than 20% more often than the average customer returns for a " +
        "store in a given state for a given year."""
    )
  )

  /**
    * Generate a list of queries based on a list of ids passed on as argument but when not provided then return all queries.
    *
    * @param args
    * @return
    */
  def generateQueries(args: Args): Array[Query] = {
    val queryIds: Set[Short] = args
      .get("ids")
      .map(_.split(",").map(_.toShort).toSet)
      .getOrElse(QueryFactory.queries.keySet)

    QueryFactory.queries.filterKeys(id => queryIds.contains(id)).values.toArray
  }

  def makeQuery(id: Short, businessQuestion: String, queryClass: QueryClass = UNKNOWN): (Short, Query) = {
    (id, Query(
      id,
      businessQuestion,
      new File(getClass.getResource(s"/queries/query$id.sql").getPath),
      new File(getClass.getResource(s"/answer_sets").getPath)
        .listFiles
        .filter(f => f.getName == s"$id.ans" || f.getName == s"${id}_NULLS_FIRST.ans")
        .head,
      queryClass
    ))
  }
}

