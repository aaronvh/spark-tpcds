package org.avanhecken.tpcds

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkTPCDSTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  var spark: SparkSession = _
  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("SparkTPCDSTest")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.close()
  }

  "benchmark" should "run first query successfully" in {
    SparkTPCDS.main(Array("test", "Test if query 1 runs successfully", "tpcds", "1"))

    //spark.table("tpcds.spark_tpcds_runs").show(false)
    spark.table("tpcds.spark_tpcds_runs_summary").show(false)
  }
}
