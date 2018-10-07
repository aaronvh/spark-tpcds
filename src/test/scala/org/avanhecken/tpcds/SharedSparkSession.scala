package org.avanhecken.tpcds

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait SharedSparkSession {
  //private val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
  //rootLogger.setLevel(Level.ERROR)

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
