package org.avanhecken.tpcds

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkTPCDSTest extends FlatSpec with Matchers with BeforeAndAfterAll with SharedSparkSession {
  override def afterAll(): Unit = {
    spark.close()
  }

  "execute" should "run first query successfully" in {
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", "/home/aaronvanhecken/tpc-ds/resources", "1"))

    spark.table("tpcds.spark_tpcds_runs").show(false)
    spark.table("tpcds.spark_tpcds_runs_summary").show(false)
  }

  "list" should "print out the names of all runs" in {
    SparkTPCDS.main(Array("list", "tpcds"))
  }

  "compare" should "print out the comparison between two runs" in {
    // TO finish
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", "/home/aaronvanhecken/tpc-ds/resources", "1"))
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", "/home/aaronvanhecken/tpc-ds/resources", "1"))
    SparkTPCDS.main(Array("compare", "tpcds", "test1", "test2"))
  }
}
