package org.avanhecken.tpcds.run

import org.joda.time.{DateTime, DateTimeZone}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query._
import org.avanhecken.tpcds.ArgumentParser.Args

case class Run(name: String, description: String, database: String, executionDateTime: Long, sparkConfig: Map[String, String], queries: Array[Query]) extends SharedSparkSession {
  def execute(runDataManager: RunDataManager): Unit = {
    def prepareRun(run: Run, runDataManager: RunDataManager): Run = {
      /** If run exists then rename with the execution date expressed in milliseconds attached as suffix. */
      if (runDataManager.exists(run.name)) {
        val newName = s"${run.name}_${run.executionDateTime}"
        println(s"WARN The run already exists, renaming it from '${run.name}' to '$newName'.")
        run.copy(newName)
      } else {
        println(s"TRACE The run '${run.name}' does not exist.")
        run
      }
    }

    val preparedRun: Run = prepareRun(this, runDataManager)

    println(s"DEBUG Saving run '$name' ...")
    runDataManager.save(preparedRun)
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
