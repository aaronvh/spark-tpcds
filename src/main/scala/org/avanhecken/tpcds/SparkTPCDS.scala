package org.avanhecken.tpcds

import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.run.Run

object SparkTPCDS {
  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs: Args = ArgumentParser.parse(args)
      parsedArgs("command") match {
        case "list" => Run.list()
        case "execute" => Run.execute(parsedArgs)
        case "compare" => Run.compare(parsedArgs)
      }
    } catch {
      case e: Exception =>
        println(s"ERROR ${e.getMessage}\n${e.printStackTrace()}")
        System.exit(1)
    }
  }
}

