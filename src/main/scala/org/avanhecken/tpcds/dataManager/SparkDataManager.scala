package org.avanhecken.tpcds.dataManager

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, split}
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query.QueryResult
import org.avanhecken.tpcds.run.{Run, RunResult}
import org.avanhecken.tpcds.statement.StatementResult

class SparkDataManager(args: Args) extends DataManager with SharedSparkSession with LazyLogging {
  import spark.implicits._

  val database: String = args("database")
  spark.sql(s"create database if not exists $database")

  val runsName: String = args.getOrElse("runs", "spark_tpcds_runs")
  val runsTable: String = s"$database.$runsName"
  val statementsName: String = args.getOrElse("statements", "spark_tpcds_statements")
  val statementsTable: String = s"$database.$statementsName"

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
        val queryResults: Map[String, QueryResult] = run.queries.map {
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
    import spark.implicits._
    case class StatementResultToDF(run: String, query: String, statement: String, elapsedTime: Long)

    if (runExists(name)) {
      statements
        .where(col("statement.id").startsWith(s"$name."))
        .map{
          sr =>
            val splitId = sr.statement.id.split("\\.")
            (splitId(0), splitId(1), splitId(2), sr.elapsedTime)
        }
        .toDF("run", "query", "statement", "elapsed_time")
    } else {
      throw new RuntimeException(s"Run '$name' does not exist!")
    }
  }
}

object SparkDataManager {
  def apply(args: Args): SparkDataManager = new SparkDataManager(args)
}

