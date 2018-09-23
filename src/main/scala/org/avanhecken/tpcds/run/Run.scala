package org.avanhecken.tpcds.run

import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query._
import org.joda.time.{DateTime, DateTimeZone}

import scala.org.avanhecken.tpcds.ArgumentParser.Args

case class Run(_name: String, description: String, executionDateTime: DateTime, runDataManager: RunDataManager, args: Args) extends SharedSparkSession {
  val queries: Array[Query] = QueryFactory.generateQueries(args)

  def name: String = _name

  def execute(): Unit = {
    val database: String = args("db")

    println(s"TRACE Use '$database'.")
    spark.sql(s"use $database")

    println(s"INFO Start run '$name' ...")
    val result = RunResult(
      this,
      queries.map(query => (query.id, query.execute())).toMap
    )
    println(s"INFO Finished run '$name'.")

    /** Store the run results using a DataManager. */
    runDataManager.save(result)
  }
}

case object Run {
  def apply(args: Args): Run = {
    val argsName = args("name")
    val description = args("description")
    val executionDateTime = DateTime.now(DateTimeZone.forID("Europe/Brussels"))

    val runDataManager: RunDataManager = SparkRunDataManager(args)

    /** For now the Spark run data manager is used. */
    val run = Run(argsName, description, executionDateTime, runDataManager, args)

    /** If run exists then rename with the execution date expressed in milliseconds attached as suffix. */
    if (runDataManager.exists(run)) {
      val newName = s"${argsName}_${executionDateTime.getMillis}"
      println(s"WARN The run already exists, renaming it from '$argsName' to '$newName'.")
      run.copy(newName)
    } else {
      println(s"TRACE The run '$argsName' does not exist.")
      run
    }
  }
}
