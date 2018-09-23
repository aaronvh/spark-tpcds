package org.avanhecken.tpcds.statement

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class StatementListener(statement: Statement) extends SparkListener {
  var startEvent: Option[SparkListenerSQLExecutionStart] = None
  var endEvent: Option[SparkListenerSQLExecutionEnd] = None

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => startEvent = Some(e)
      case e: SparkListenerSQLExecutionEnd => endEvent= Some(e)
    }
  }

  def getStatementResult(): StatementResult = StatementResult(statement, startEvent, endEvent)
}
