package org.avanhecken.tpcds.query

import java.io.File

import org.avanhecken.tpcds.ArgumentParser.Args
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
  def statements(args: Args): Array[Statement] = {
    val resourceLocation: String = args("resource_location")
    val sqlFileLocation: File = new File(s"$resourceLocation/queries/query$id.sql")

    implicit val codec = Codec("UTF-8")
    Source.fromFile(sqlFileLocation.getPath).mkString.trim.split(";").map(Statement)
  }

  def execute(args: Args): QueryResult = {
    print(s"INFO Run query '$id' ... ")
    val result = QueryResult(
      this,
      statements(args).zipWithIndex.map { case (statement, index) => statement.execute(index) }
    )
    println(s"INFO Finished query '$id'.")

    result
  }
}
