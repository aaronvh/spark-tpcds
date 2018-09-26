package org.avanhecken.tpcds

import org.avanhecken.tpcds.run.Run

object SparkTPCDS {
  def main(args: Array[String]): Unit = {
    try {
      Run.execute(ArgumentParser.parse(args))
    } catch {
      case e: Exception =>
        println(s"ERROR ${e.getMessage}\n${e.printStackTrace()}")
        System.exit(1)
    }
  }
}

