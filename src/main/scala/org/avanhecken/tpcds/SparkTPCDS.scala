package org.avanhecken.tpcds

import org.avanhecken.tpcds.run.Run
import scala.org.avanhecken.tpcds.ArgumentParser

object SparkTPCDS {
  def main(args: Array[String]): Unit = {
    Run(ArgumentParser.parse(args)).execute()
  }
}

