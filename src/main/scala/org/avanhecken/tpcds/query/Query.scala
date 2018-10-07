package org.avanhecken.tpcds.query

import org.avanhecken.tpcds.SparkTPCDS
import org.avanhecken.tpcds.dataManager.DataManager
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
  private val appLogger = SparkTPCDS.appLogger

  def execute(runDataManager: DataManager): Unit = {
    appLogger.info(s"Run query '$id' ... ")
    statements.foreach(_.execute(runDataManager))
    appLogger.info(s"Finished query '$id'.")
  }
}
