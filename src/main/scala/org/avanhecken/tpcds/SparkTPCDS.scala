package org.avanhecken.tpcds

import org.avanhecken.tpcds.run.Run
import scala.org.avanhecken.tpcds.ArgumentParser

object SparkTPCDS {
  def main(args: Array[String]): Unit = {
    try {
      Run(ArgumentParser.parse(args)).execute()
    } catch {
      case e: Exception =>
        println(s"ERROR ${e.getMessage}\n${e.printStackTrace()}")
        System.exit(1)
    }
  }
}

