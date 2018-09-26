package org.avanhecken.tpcds.query

import java.io.File

import org.avanhecken.tpcds.query.QueryFactory.getClass

import scala.io.{Codec, Source}
import org.avanhecken.tpcds.statement.Statement

/**
  * Corresponds to each of the 99 queries in the TPC-DS benchmark.
  *
  * @param id
  * @param businessQuestion
  * @param sqlFileLocation
  * @param answerFileLocation
  * @param queryClass
  */
case class Query(id: Short, businessQuestion: String, queryClass: QueryClass) {
  lazy val statements: Array[Statement] = {
    val sqlFileLocation: File = new File(getClass.getResource(s"/queries/query$id.sql").getPath)

    implicit val codec = Codec("UTF-8")
    Source.fromFile(sqlFileLocation.getPath).mkString.trim.split(";").map(Statement)
  }

  def execute(): QueryResult = {
    print(s"INFO Run query '$id' ... ")
    val result = QueryResult(
      this,
      statements.zipWithIndex.map { case (statement, index) => statement.execute(index) }
    )
    println(s"INFO Finished query '$id'.")

    result
  }
}
