package org.avanhecken.tpcds

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  @transient lazy val spark = SparkSession
    .builder
    .appName("SparkPerformanceTester")
    .master("local")
    .enableHiveSupport()
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  @transient lazy val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
}
