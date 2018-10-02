package org.avanhecken.tpcds.query

import org.avanhecken.tpcds.run.RunDataManager
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
case class Query(id: Short, businessQuestion: String, queryClass: QueryClass, statements: Array[Statement]) {
  def execute(runDataManager: RunDataManager): Unit = {
    print(s"INFO Run query '$id' ... ")
    statements.foreach(_.execute(runDataManager))
    println(s"INFO Finished query '$id'.")
  }
}
