package org.avanhecken.tpcds

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkTPCDSTest extends FlatSpec with Matchers with BeforeAndAfterAll with SharedSparkSession {
  val database: String = "tpcds"
  val runsTable: String = s"$database.spark_tpcds_runs"
  val runsSummaryTable: String = s"$database.spark_tpcds_runs_summary"

  val resourceLocation: String = getClass.getResource("/").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql(s"create database if not exists $database")
    spark.sql(s"drop table if exists $runsTable")
    spark.sql(s"drop table if exists $runsSummaryTable")

    import spark.implicits._
    spark.sql(s"drop table if exists $database.test1")
    Range(1, 100).toList.toDF("col1").write.saveAsTable(s"$database.test1")
    spark.sql(s"drop table if exists $database.test2")
    Range(1, 10000).toList.toDF("col1").write.saveAsTable(s"$database.test2")
  }

  override def afterAll(): Unit = {
    spark.close()

    super.afterAll()
  }

  "execute" should "run first query successfully" in {
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", resourceLocation, "1"))

    spark.table(runsTable).show(false)
  }

  "list" should "print out the names of all runs" in {
    SparkTPCDS.main(Array("execute", "test1", "Test1 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test2", "Test2 if query 1 runs successfully", "tpcds", resourceLocation, "2"))
    SparkTPCDS.main(Array("list", "tpcds"))
  }

  "compare" should "print out the comparison between two equal runs" in {
    // TO finish
    SparkTPCDS.main(Array("execute", "test3", "Test3 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test4", "Test4 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("compare", "tpcds", "test3", "test4"))
  }

  it should "print out the comparison between two non-equal runs" in {
    // TO finish
    SparkTPCDS.main(Array("execute", "test5", "Test5 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test6", "Test6 if query 1 runs successfully", "tpcds", resourceLocation, "2"))
    SparkTPCDS.main(Array("compare", "tpcds", "test5", "test6"))
  }
}
