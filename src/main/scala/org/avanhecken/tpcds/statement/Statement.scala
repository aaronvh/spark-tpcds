package org.avanhecken.tpcds.statement

import org.apache.spark.sql.Row
import org.avanhecken.tpcds.SharedSparkSession

case class Statement(text: String) extends SharedSparkSession {
  def execute(index: Long): StatementResult = {
    println(s"TRACE Create listener for statement '$index'.")
    val listener = new StatementListener(this)

    try {
      println("TRACE Add listener.")
      sc.addSparkListener(listener)
      println(s"DEBUG Execute statement '$index' ...")
      /** Collect the result, it can be used to compare with the expected answer test and determine if the count is correct.
        * Downside, possible OoM issues!
        * Maybe another action will be preferred in the future.
        * Like:
        * - show -> To compare with the answer test but no direct count.
        * - count -> Unable to compare with the answer tests.
        */
      val result: Array[Row] = spark.sql(text).collect
      println(s"DEBUG Statement '$index' finished.")
      println(s"DEBUG Print result:")
      result.foreach(println)
      println(s"DEBUG Result count is '${result.size}'")
      println(s"DEBUG Statement '$index' finished.")
      listener.getStatementResult
    } catch {
      case e: Exception =>
        println(s"DEBUG Statement '$index' failed!")
        println(s"ERROR ${e.getMessage}\n${e.printStackTrace()}") // Same at main method!
        StatementResult(this)
    } finally {
      println("TRACE Remove listener.")
      sc.removeSparkListener(listener)
    }
  }
}
