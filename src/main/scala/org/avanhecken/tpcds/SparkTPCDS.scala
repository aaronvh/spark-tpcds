package org.avanhecken.tpcds

import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.query.QueryResult
import org.avanhecken.tpcds.run.{Run, RunDataManager, RunResult, SparkRunDataManager}

object SparkTPCDS {
  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs: Args = ArgumentParser.parse(args)

      /** For now the Spark run data manager is used. */
      val runDataManager: RunDataManager = SparkRunDataManager(parsedArgs)

      parsedArgs("command") match {
        case "list" => list(runDataManager)
        case "execute" => execute(runDataManager, parsedArgs)
        case "compare" => compare(runDataManager, parsedArgs)
      }
    } catch {
      case e: Exception =>
        println(s"ERROR ${e.getMessage}\n${e.printStackTrace()}")
        System.exit(1)
    }
  }

  def list(runDataManager: RunDataManager): Unit = {
    runDataManager.getNames().foreach(println)
  }

  def execute(runDataManager: RunDataManager, args: Args): Unit = {
    val result: RunResult = Run(args).execute(runDataManager, args)

    /** Store the run results using a DataManager. */
    runDataManager.save(result)
  }

  def compare(runDataManager: RunDataManager, args: Args) = {
    val name1: String = args("name1")
    val name2: String = args("name2")

    val queryResults1: Map[Short, QueryResult] = runDataManager.get(name1).queryResults
    val queryResults2: Map[Short, QueryResult] = runDataManager.get(name2).queryResults

    val allKeys = (queryResults1.keySet ++ queryResults2.keySet).toArray.sorted

    allKeys.foreach {
      key =>
        println(s"Query $key")
        (queryResults1.get(key), queryResults2.get(key)) match {
          case (None, Some(_)) => println("  No result found in run 1.")
          case (Some(_), None) => println("  No result found in run 2.")
          case (Some(queryResult1), Some(queryResult2)) =>
            println(s"Elapsed time matches               : ${queryResult1.elapsedTime == queryResult2.elapsedTime}")
            queryResult1.statementResults.zip(queryResult2.statementResults).foreach {
              case (sr1, sr2) =>
                println(s"Description matches              : ${sr1.description == sr2.description}")
                println(s"Details matches                  : ${sr1.details == sr2.details}")
                println(s"Physical plan description matches: ${sr1.physicalPlanDescription == sr2.physicalPlanDescription}")
            }
        }
    }
  }
}

