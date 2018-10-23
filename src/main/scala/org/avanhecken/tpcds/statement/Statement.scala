package org.avanhecken.tpcds.statement

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Row
import org.avanhecken.tpcds.{SharedSparkSession, SparkTPCDS}
import org.avanhecken.tpcds.dataManager.DataManager

case class Statement(id: String, text: String) extends SharedSparkSession with LazyLogging {
  def execute(runDataManager: DataManager): Unit = {
    logger.trace(s"Create listener for statement '$id'.")
    val listener = new StatementListener(this)

    val statementResult = try {
      logger.trace("Add listener.")
      sc.addSparkListener(listener)
      logger.info(s"Execute statement '$id' ...")
      /** Collect the result, it can be used to compare with the expected answer test and determine if the count is correct.
        * Downside, possible OoM issues!
        * Maybe another action will be preferred in the future.
        * Like:
        * - show -> To compare with the answer test but no direct count.
        * - count -> Unable to compare with the answer tests.
        */
      val result: Array[Row] = spark.sql(text).collect
      logger.info(s"Statement '$id' finished.")
      logger.debug(s"Print result:")
      result.foreach(r => logger.debug(r.mkString(" | ")))
      logger.debug(s"Result count is '${result.size}'")
      listener.getStatementResult
    } catch {
      case e: Exception =>
        logger.error(s"Statement '$id' failed!")
        logger.error(s"${e.getMessage}\n${e.printStackTrace()}") // Same at main method!
        StatementResult(this)
    } finally {
      logger.trace("Remove listener.")
      sc.removeSparkListener(listener)
    }

    logger.debug(s"Saving statement '$id' result ...")
    runDataManager.save(statementResult)
    logger.debug(s"Saved statement '$id' result.")
  }
}
