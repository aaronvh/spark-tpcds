package org.avanhecken.tpcds

import org.avanhecken.tpcds.dataManager.SparkDataManager
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait SparkFlatSpec extends FlatSpec with BeforeAndAfterAll with SharedSparkSession {
  val dataManager: SparkDataManager = SparkDataManager(Map("database" -> "tpcds"))

  val resourceLocation: String = getClass.getResource("/").getPath

  val database = dataManager.database
  val runsTable = dataManager.runsTable
  val statementsTable = dataManager.statementsTable

  override def beforeAll(): Unit = {
    spark.sql(s"create database if not exists $database")
    spark.sql(s"drop table if exists $runsTable")
    spark.sql(s"drop table if exists $statementsTable")

    import spark.implicits._
    spark.sql(s"drop table if exists $database.test1")
    Range(1, 100).toList.toDF("col1").write.saveAsTable(s"$database.test1")
    spark.sql(s"drop table if exists $database.test2")
    Range(1, 10000).toList.toDF("col1").write.saveAsTable(s"$database.test2")
  }

  override def afterAll(): Unit = {
    import spark.implicits._
    spark.sql(s"show tables in $database")
      .select("tableName")
      .as[String]
      .collect
      .foreach(tableName => spark.sql(s"drop table $tableName"))
    spark.sql(s"drop database if exists $database")
    spark.close()
  }
}
