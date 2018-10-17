package org.avanhecken.tpcds.run

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeZone}
import org.avanhecken.tpcds.{SharedSparkSession, SparkTPCDS}
import org.avanhecken.tpcds.query._
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.dataManager.DataManager

case class Run(name: String, description: String, database: String, executionDateTime: Long, sparkConfig: Map[String, String], queries: Array[Query]) extends SharedSparkSession with LazyLogging {
  def execute(runDataManager: DataManager): Unit = {
//    def validateRun(run: Run, runDataManager: DataManager): Run = {
//      if (runDataManager.exists(run.name)) {
//        throw new RuntimeException("Run already exists!")
//      } else {
//        logger.trace(s"The run '${run.name}' does not exist.")
//        run
//      }
//    }

//    val validRun: Run = validateRun(this, runDataManager)

    logger.debug(s"Saving run '$name' ...")
//    runDataManager.save(validRun)
    runDataManager.save(this)
    logger.debug(s"Saved run '$name'.")

    logger.info(s"Start run '$name' ...")
    spark.sql(s"use $database")
    queries.foreach(_.execute(runDataManager))
    logger.info(s"Finished run '$name'.")
  }
}

case object Run extends SharedSparkSession {
  def apply(args: Args): Run = {
    val name: String = args("name")
    val description: String = args("description")
    val database: String = args("database")
    val executionDateTime: Long = DateTime.now(DateTimeZone.forID("Europe/Brussels")).getMillis
    val queries: Array[Query] = new QueryFactory(args).generateQueries(name)

    Run(name, description, database, executionDateTime, spark.conf.getAll, queries)
  }
}
