package org.avanhecken.tpcds.run

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.avanhecken.tpcds.SharedSparkSession
import org.avanhecken.tpcds.query.QueryFactory
import scala.org.avanhecken.tpcds.ArgumentParser.Args

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

  val database: String = args("db")

  // @TODO -> Can be set by an option.
  //val dsName: String = "spark_tpcds_runs"
  //val dsTableName: String = s"$database.$dsName"

  // @TODO -> Can be set by an option.
  val dfName: String = "spark_tpcds_runs_summary"
  val dfTableName: String = s"$database.$dfName"
  val dfSchema: StructType =
    StructType {
      List(
        StructField("run", StringType, false),
        StructField("total", LongType, true)
      ) ++ QueryFactory.queries.keySet.map(id => StructField(s"query$id", LongType, true)).toList
    }

  //lazy val runDs: Dataset[RunResult] = spark.table(dsTableName).as[RunResult]
  lazy val runDf: DataFrame = spark.table(dfTableName)

  private def initializeDb(): SparkRunDataManager = {
    /** DS */
    //spark.emptyDataset[RunResult].write.mode(SaveMode.Ignore).saveAsTable(dsTableName)

    /** DF */
    // @TODO -> Check if table already exists.
    // If yes but with different schema then rename the table and create a new one with the new schema.
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
    //!(runDs.filter(_.run.name == run.name).count() == 0)
    !(runDf.where('run.eqNullSafe(run.name)).count() == 0)
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
      //val ds: Dataset[RunResult] = List(runResult).toDS
      //ds.write.insertInto(dsTableName)

      val allIds: Set[Short] = QueryFactory.queries.keySet
      val runElapsedTimes: Map[Short, Long] = runResult.queryResults.map { case (id, res) => (id, res.elapsedTime) }

      val elapsedTimes: Array[Long] = if (runResult.queryResults.size != allIds.size) {
        allIds.map(id => runElapsedTimes.getOrElse(id, -2L)).toArray
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
  def apply(args: Args): SparkRunDataManager = {
    new SparkRunDataManager(args).initializeDb()
  }
}

