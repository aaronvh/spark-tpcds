package org.avanhecken.tpcds.statement

import org.apache.spark.sql.Row
import org.avanhecken.tpcds.{SharedSparkSession, SparkTPCDS}
import org.avanhecken.tpcds.dataManager.DataManager

case class Statement(id: String, text: String) extends SharedSparkSession {
  private val appLogger = SparkTPCDS.appLogger

  def execute(runDataManager: DataManager): Unit = {
    appLogger.trace(s"Create listener for statement '$id'.")
    val listener = new StatementListener(this)

    val statementResult = try {
      appLogger.trace("Add listener.")
      sc.addSparkListener(listener)
      appLogger.debug(s"Execute statement '$id' ...")
      /** Collect the result, it can be used to compare with the expected answer test and determine if the count is correct.
        * Downside, possible OoM issues!
        * Maybe another action will be preferred in the future.
        * Like:
        * - show -> To compare with the answer test but no direct count.
        * - count -> Unable to compare with the answer tests.
        */
      val result: Array[Row] = spark.sql(text).collect
      appLogger.debug(s"Statement '$id' finished.")
      appLogger.debug(s"Print result:")
      result.foreach(println)
      appLogger.debug(s"Result count is '${result.size}'")
      appLogger.debug(s"Statement '$id' finished.")
      listener.getStatementResult
    } catch {
      case e: Exception =>
        appLogger.debug(s"Statement '$id' failed!")
        appLogger.error(s"${e.getMessage}\n${e.printStackTrace()}") // Same at main method!
        StatementResult(this)
    } finally {
      appLogger.trace("Remove listener.")
      sc.removeSparkListener(listener)
    }

    appLogger.debug(s"Saving statement '$id' result ...")
    runDataManager.save(statementResult)
    appLogger.debug(s"Saved statement '$id' result.")
  }
}
