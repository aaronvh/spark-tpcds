package org.avanhecken.tpcds.run

import org.joda.time.{DateTime, DateTimeZone}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query._
import org.avanhecken.tpcds.ArgumentParser.Args

case class Run(name: String, description: String, database: String, executionDateTime: Long, sparkConfig: Map[String, String], queries: Array[Query]) extends SharedSparkSession {
  def execute(runDataManager: RunDataManager): Unit = {
    def validateRun(run: Run, runDataManager: RunDataManager): Run = {
      if (runDataManager.exists(run.name)) {
        throw new RuntimeException("Run already exists!")
      } else {
        println(s"TRACE The run '${run.name}' does not exist.")
        run
      }
    }

    val validRun: Run = validateRun(this, runDataManager)

    println(s"DEBUG Saving run '$name' ...")
    runDataManager.save(validRun)
    println(s"DEBUG Saved run '$name'.")

    println(s"INFO Start run '$name' ...")
    spark.sql(s"use $database")
    queries.foreach(_.execute(runDataManager))
    println(s"INFO Finished run '$name'.")
  }
}

case object Run extends SharedSparkSession {
  def apply(args: Args): Run = {
    val name: String = args("name")
    val description: String = args("description")
    val database: String = args("database")
    val executionDateTime: Long = DateTime.now(DateTimeZone.forID("Europe/Brussels")).getMillis
    val queries: Array[Query] = QueryFactory.generateQueries(name, args)

    Run(name, description, database, executionDateTime, spark.conf.getAll, queries)
  }
}
