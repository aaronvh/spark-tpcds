package org.avanhecken.tpcds

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkTPCDSTest extends FlatSpec with Matchers with BeforeAndAfterAll with SharedSparkSession {
  override def afterAll(): Unit = {
    spark.close()
  }

  "benchmark" should "run first query successfully" in {
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", "/home/aaronvanhecken/tpc-ds/resources", "1"))

    spark.table("tpcds.spark_tpcds_runs").show(false)
    spark.table("tpcds.spark_tpcds_runs_summary").show(false)
  }
}
