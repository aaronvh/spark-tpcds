package org.avanhecken.tpcds.statement

case class StatementResult(statement: Statement, startTime: Long = -1L, endTime: Long = -1L, description: String = "", details: String = "", physicalPlanDescription: String = "") {
  def elapsedTime: Long = endTime - startTime
}
