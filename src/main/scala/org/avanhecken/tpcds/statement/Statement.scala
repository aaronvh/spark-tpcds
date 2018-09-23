package org.avanhecken.tpcds.statement

import org.avanhecken.tpcds.SharedSparkSession

case class Statement(text: String) extends SharedSparkSession {
  def execute(index: Long): StatementResult = {
    val listener = new StatementListener(this)

    sc.addSparkListener(listener)
    try {
      spark.sql(text)
      // @TODO -> log as debug
      println(s"\nStatement $index finished.")
      listener.getStatementResult()
    } catch {
      case e: Exception =>
        // @TODO -> log as debug
        println(s"\nStatement $index failed!")
        println(e.getStackTrace)
        StatementResult(this, None, None)
    } finally {
      sc.removeSparkListener(listener)
    }
  }
}
