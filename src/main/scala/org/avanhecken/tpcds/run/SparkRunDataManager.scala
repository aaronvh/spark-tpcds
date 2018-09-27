package org.avanhecken.tpcds.run

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query.QueryFactory
import org.avanhecken.tpcds.ArgumentParser.Args

/**
  * Runtable schema:
  *   StructType(Array(
  *     StructField("run",
  *   ))
  *
  * @param args
  */
class SparkRunDataManager(override val args: Args) extends RunDataManager with SharedSparkSession {
  import spark.implicits._

  val database: String = args("database")

  // @TODO -> Can be set by an option.
  val dsName: String = "spark_tpcds_runs"
  val dsTableName: String = s"$database.$dsName"

  // @TODO -> Can be set by an option.
  val dfName: String = "spark_tpcds_runs_summary"
  val dfTableName: String = s"$database.$dfName"
  val dfSchema: StructType =
    StructType {
      Array(
        StructField("run", StringType, false),
        StructField("total", LongType, true)
      ) ++ QueryFactory.ids.map(id => StructField(s"query$id", LongType, true))
    }

  lazy val runDs: Dataset[RunResult] = spark.table(dsTableName).as[RunResult]
  lazy val runDf: DataFrame = spark.table(dfTableName)

  /**
    * Check if the run table exist and if the schema is correct.
    *
    * @return the initialized Run DataManager
    */
  override def initialize(): SparkRunDataManager = {
    /** DS */
    import spark.implicits._
    spark.emptyDataset[RunResult].write.mode(SaveMode.Ignore).saveAsTable(dsTableName)

    /** DF */
    spark.createDataFrame(sc.emptyRDD[Row], dfSchema).write.mode(SaveMode.Ignore).saveAsTable(dfTableName)

    this
  }

  /**
    * Check if the run corresponds to an already existing one.
    *
    * @param run
    * @return true if run exists else false
    */
  override def exists(run: Run): Boolean = {
    val name: String = run.name

    !(runDs.filter(_.run.name == name).count() == 0) ||
    !(runDf.where('run.eqNullSafe(name)).count() == 0)
  }

  /**
    * Save the run if it does not exists.
    *
    * @param run
    */
  override def save(runResult: RunResult): Unit = {
    if (exists(runResult.run)) {
      throw new RuntimeException(s"Run '${runResult.run.name}' already exists!")
    } else {
      val ds: Dataset[RunResult] = List(runResult).toDS
      ds.write.insertInto(dsTableName)

      val runElapsedTimes: Map[Short, Long] = runResult.queryResults.map { case (id, res) => (id, res.elapsedTime) }

      val elapsedTimes: Array[Long] = if (runResult.queryResults.size != QueryFactory.ids.length) {
        QueryFactory.ids.map(id => runElapsedTimes.getOrElse(id, -2L))
      } else {
        runElapsedTimes.values.toArray
      }

      val row = sc.parallelize(Seq(Row.fromSeq(Array(runResult.run.name, runResult.elapsedTime) ++ elapsedTimes)))
      val df = spark.createDataFrame(row, dfSchema)
      df.write.insertInto(dfTableName)
    }
  }
}

object SparkRunDataManager {
  def apply(args: Args): SparkRunDataManager = new SparkRunDataManager(args).initialize()
}

