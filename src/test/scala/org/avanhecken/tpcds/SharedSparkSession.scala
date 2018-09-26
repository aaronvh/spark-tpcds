package org.avanhecken.tpcds

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  val spark = SparkSession.builder
    .appName("SparkPerformanceTester")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
}
