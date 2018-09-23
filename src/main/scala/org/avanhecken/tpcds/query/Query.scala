package org.avanhecken.tpcds.query

import java.io.File

import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.statement.Statement
import query.QueryClass

import scala.io.{Codec, Source}

/**
  * Corresponds to each of the 99 queries in the TPC-DS benchmark.
  *
  * @param id
  * @param businessQuestion
  * @param sqlFileLocation
  * @param answerFileLocation
  * @param queryClass
  */
case class Query(id: Short, businessQuestion: String, sqlFileLocation: File, answerFileLocation: File, queryClass: QueryClass) {
  lazy val statements: Array[Statement] = {
    implicit val codec = Codec("UTF-8")
    Source.fromFile(sqlFileLocation.getPath).mkString.trim.split(";").map(Statement)
  }

  def execute(): QueryResult = {
    QueryResult(
      this,
      statements.zipWithIndex.map { case (statement, index) => statement.execute(index) }
    )
  }
}
