package org.avanhecken.tpcds.run

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query.{QueryFactory, QueryResult}
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.statement.StatementResult

class SparkRunDataManager(override val args: Args) extends RunDataManager with SharedSparkSession {
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

  lazy val runs: Dataset[Run] = spark.table(runsTable).as[Run]
  lazy val statements: Dataset[StatementResult] = spark.table(statementsTable).as[StatementResult]
  lazy val runsSummary: DataFrame = spark.table(runsSummaryTable)

  override def initialize(): SparkRunDataManager = {
    /** DS */
    import spark.implicits._
    spark.emptyDataset[Run].write.mode(SaveMode.Ignore).saveAsTable(runsTable)
    spark.emptyDataset[StatementResult].write.mode(SaveMode.Ignore).saveAsTable(statementsTable)

    /** DF */
    spark.createDataFrame(sc.emptyRDD[Row], runsSummarySchema).write.mode(SaveMode.Ignore).saveAsTable(runsSummaryTable)

    this
  }

  override def exists(name: String): Boolean = {
    !(runs.filter(_.name == name).count() == 0)
  }

  override def save(run: Run): Unit = {
    if (exists(run.name)) {
      throw new RuntimeException(s"Run '${run.name}' already exists!")
    } else {
      val ds: Dataset[Run] = List(run).toDS
      ds.write.insertInto(runsTable)

      //val runElapsedTimes: Map[Short, Long] = runResult.queryResults.map { case (id, res) => (id, res.elapsedTime) }

      //val elapsedTimes: Array[Long] = if (runResult.queryResults.size != QueryFactory.ids.length) {
      //  QueryFactory.ids.map(id => runElapsedTimes.getOrElse(id, -2L))
      //} else {
      //  runElapsedTimes.values.toArray
      //}

      //val row = sc.parallelize(Seq(Row.fromSeq(Array(runResult.run.name, runResult.elapsedTime) ++ elapsedTimes)))
      //val df = spark.createDataFrame(row, dfSchema)
      //df.write.insertInto(dfTableName)
    }
  }

  override def save(statementResult: StatementResult): Unit = {
    val ds: Dataset[StatementResult] = List(statementResult).toDS
    ds.write.insertInto(statementsTable)
  }

  override def get(name: String): RunResult = {
    val run: Run = runs.filter(_.name == name).head()
    val statementResults: Array[StatementResult] = ???
    val queryResults: Array[QueryResult] = run.queries.map(QueryResult(query, ))

    RunResult(run, )
  }

  override def getNames(): Array[String] = {
    runs.map(_.name).collect
  }
}

object SparkRunDataManager {
  def apply(args: Args): SparkRunDataManager = new SparkRunDataManager(args).initialize()
}

