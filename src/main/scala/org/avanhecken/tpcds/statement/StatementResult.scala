package org.avanhecken.tpcds.statement

import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

case class StatementResult(statement: Statement, startEvent: Option[SparkListenerSQLExecutionStart], endEvent: Option[SparkListenerSQLExecutionEnd]) {
  def elapsedTime: Long = (startEvent, endEvent) match {
    case (Some(se), Some(ee)) => ee.time - se.time
    case _ => -1L
  }
}
