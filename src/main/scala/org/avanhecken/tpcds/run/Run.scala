package org.avanhecken.tpcds.run

import org.joda.time.{DateTime, DateTimeZone}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query._
import org.avanhecken.tpcds.ArgumentParser.Args

case class Run(name: String, description: String, executionDateTime: Long) extends SharedSparkSession {
  def execute(args: Args): RunResult = {
    val queries: Array[Query] = QueryFactory.generateQueries(args)
    val database: String = args("db")

    println(s"TRACE Use '$database'.")
    spark.sql(s"use $database")

    println(s"INFO Start run '$name' ...")
    val result = RunResult(this, queries.map(query => (query.id, query.execute())).toMap)
    println(s"INFO Finished run '$name'.")

    result
  }
}

case object Run {
  def execute(args: Args): Unit = {
    val argsName: String = args("name")
    val description: String = args("description")
    val executionDateTime: Long = DateTime.now(DateTimeZone.forID("Europe/Brussels")).getMillis

    val run = Run(argsName, description, executionDateTime)

    /** For now the Spark run data manager is used. */
    val runDataManager: RunDataManager = SparkRunDataManager(args)

    val result = prepareRun(run, runDataManager).execute(args)

    /** Store the run results using a DataManager. */
    runDataManager.save(result)
  }

  def prepareRun(run: Run, runDataManager: RunDataManager): Run = {
    /** If run exists then rename with the execution date expressed in milliseconds attached as suffix. */
    if (runDataManager.exists(run)) {
      val newName = s"${run.name}_${run.executionDateTime}"
      println(s"WARN The run already exists, renaming it from '${run.name}' to '$newName'.")
      run.copy(newName)
    } else {
      println(s"TRACE The run '${run.name}' does not exist.")
      run
    }
  }
}
