package org.avanhecken.tpcds.query

import java.io.File

import org.avanhecken.tpcds.statement.StatementResult

/**
  * Corresponds to each of the 99 queries in the TPC-DS benchmark.
  *
  * @param id
  * @param businessQuestion
  * @param sqlFileLocation
  * @param answerFileLocation
  * @param queryClass
  */
case class QueryResult(query: Query, statementResults: Array[StatementResult]) {
  def elapsedTime: Long = statementResults.map(_.elapsedTime).sum

  def validateAnswer: Boolean = {
    val answerFileLocation = new File(getClass.getResource(s"/answer_sets").getPath)
      .listFiles
      .filter(f => f.getName == s"${query.id}.ans" || f.getName == s"${query.id}_NULLS_FIRST.ans")
      .head

    // @TODO -> Implement logic

    true
  }
}

//case object QueryResult {
//  def empty(query: Query): QueryResult = QueryResult(query, query.statements.map(StatementResult(_, None, None)))
//}