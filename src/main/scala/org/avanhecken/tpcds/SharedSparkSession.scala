package org.avanhecken.tpcds

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  @transient lazy val spark = SparkSession
    .builder
    .appName("SparkPerformanceTester")
    .enableHiveSupport()
    .getOrCreate()

  @transient lazy val sc = spark.sparkContext
}
