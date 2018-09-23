package org.avanhecken.tpcds.run

import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query._
import org.joda.time.{DateTime, DateTimeZone}

import scala.org.avanhecken.tpcds.ArgumentParser.Args

class Run(val name: String, description: String, executionDate: DateTime, runDataManager: RunDataManager, args: Args) extends SharedSparkSession {
  val queries: Array[Query] = QueryFactory.generateQueries(args)

  def execute(): Unit = {
    val database: String = args("db")

    spark.sql(s"use $database")

    val runResult = RunResult(
      this,
      queries.map {
        query =>
          print(s"Running query => ${query.id} ... ")
          (query.id, query.execute())
      }.toMap
    )

    /** Store the run results using a DataManager. */
    runDataManager.save(runResult)
  }
}

object Run {
  def apply(args: Args): Run = {
    val name = args("name")
    val description = args("description")
    val executionDateTime = DateTime.now(DateTimeZone.forID("Europe/Brussels"))

    /** For now the Spark run data manager is used. */
    new Run(name, description, executionDateTime, SparkRunDataManager(args), args)
  }
}
