package org.avanhecken.tpcds.dataManager

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.{SharedSparkSession, SparkTPCDS}
import org.avanhecken.tpcds.query.{QueryFactory, QueryResult}
import org.avanhecken.tpcds.run.{Run, RunResult}
import org.avanhecken.tpcds.statement.StatementResult

class SparkDataManager(args: Args) extends DataManager with SharedSparkSession with LazyLogging {
  import spark.implicits._

  val database: String = args("database")

  // @TODO -> Can be set by an option.
  val runsName: String = "spark_tpcds_runs"
  val runsTable: String = s"$database.$runsName"
  val statementsName: String = "spark_tpcds_statements"
  val statementsTable: String = s"$database.$statementsName"

  // @TODO -> Can be set by an option.
  val runsSummaryName: String = "spark_tpcds_runs_summary"
  val runsSummaryTable: String = s"$database.$runsSummaryName"
  val runsSummarySchema: StructType =
    StructType {
      Array(
        StructField("run", StringType, false),
        StructField("total", LongType, true)
      ) ++ QueryFactory.ids.map(id => StructField(s"query$id", LongType, true))
    }

  def runs: Dataset[Run] = spark.table(runsTable).as[Run]
  def statements: Dataset[StatementResult] = spark.table(statementsTable).as[StatementResult]

  spark.emptyDataset[Run].write.mode(SaveMode.Ignore).saveAsTable(runsTable)
  spark.emptyDataset[StatementResult].write.mode(SaveMode.Ignore).saveAsTable(statementsTable)

  private def runExists(name: String): Boolean = {
    !(runs.filter(_.name == name).count() == 0)
  }

  override def save(run: Run): Unit = {
    if (runExists(run.name)) {
      throw new RuntimeException(s"Run '${run.name}' already exists!")
    } else {
      val ds: Dataset[Run] = List(run).toDS
      ds.write.insertInto(runsTable)
    }
  }

  private def statementResultExists(id: String): Boolean = {
    !(statements.filter(_.statement.id == id).count == 0)
  }

  override def save(statementResult: StatementResult): Unit = {
    if (statementResultExists(statementResult.statement.id)) {
      throw new RuntimeException(s"StatementResult '${statementResult.statement.id}' already exists!")
    } else {
      val ds: Dataset[StatementResult] = List(statementResult).toDS
      ds.write.insertInto(statementsTable)
    }
  }

  override def get(name: String): RunResult = {
    logger.trace(s"Filter run '$name'")
    val run: Option[Run] = runs.filter(_.name == name).collect.headOption

    run match {
      case Some(run) =>
        logger.trace(s"Get query results")
        val queryResults: Map[Short, QueryResult] = run.queries.map {
          query =>
            val statementResults: Array[StatementResult] = query
              .statements
              .flatMap{
                statement =>
                  statements.filter(statementResult => statementResult.statement.id == statement.id).collect
              }
            (query.id, QueryResult(query, statementResults))
        }.toMap

        RunResult(run, queryResults)
      case None => throw new RuntimeException("Run not found!")
    }
  }

  override def getNames(): Array[String] = {
    runs.map(_.name).collect
  }

  def getDF(name: String): DataFrame = {
    if (runExists(name)) {
      val run: Dataset[Run] = runs.where('name === name)
      run.flatMap(
        r => r.queries.flatMap(
          q => q.statements.map {
            s =>
              val res = statements.where('id === s.id).head
              (r.name, q.id, s.id, res.elapsedTime)
          }
        )
      ).toDF("run", "query", "statement", "elapsed_time")
    } else {
      throw new RuntimeException(s"Run '$name' does not exist!")
    }
  }
}

object SparkDataManager {
  def apply(args: Args): SparkDataManager = new SparkDataManager(args)

  def apply(database: String): SparkDataManager = new SparkDataManager(Map("database" -> database))
}

