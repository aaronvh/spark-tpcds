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
      case _ =>
    }
  }

  def getStatementResult: StatementResult = StatementResult(
    statement,
    startEvent.map(_.time).getOrElse(-1L),
    endEvent.map(_.time).getOrElse(-1L),
    startEvent.map(_.description).getOrElse(""),
    startEvent.map(_.details).getOrElse(""),
    startEvent.map(_.physicalPlanDescription).getOrElse("")
  )
}
