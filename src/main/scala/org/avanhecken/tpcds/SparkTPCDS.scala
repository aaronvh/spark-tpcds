package org.avanhecken.tpcds

import com.typesafe.scalalogging.LazyLogging
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.dataManager.{DataManager, SparkDataManager}
import org.avanhecken.tpcds.query.QueryResult
import org.avanhecken.tpcds.run.Run
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object SparkTPCDS extends LazyLogging {
  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs: Args = ArgumentParser.parse(args)

      /** For now the Spark run data manager is used. */
      val dataManager: DataManager = SparkDataManager(parsedArgs)

      parsedArgs("command") match {
        case "list" => list(dataManager)
        case "execute" => execute(dataManager, parsedArgs)
        case "compare" => compare(dataManager, parsedArgs)
      }
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        e.printStackTrace()
        System.exit(1)
    }
  }

  def list(runDataManager: DataManager): Unit = {
    val dateTimeFormatter: DateTimeFormatter =  DateTimeFormat.forPattern("dd/MM/yyyy hh:mm:ss")

    runDataManager.getNames.foreach{
      name =>
        val executionDateTime: DateTime = new DateTime(runDataManager.get(name).run.executionDateTime)
        logger.info(f"$name%-25s ${dateTimeFormatter.print(executionDateTime)}%25s")
    }
  }

  def execute(runDataManager: DataManager, args: Args): Unit = {
    Run(args).execute(runDataManager)
  }

  def compare(runDataManager: DataManager, args: Args): Unit = {
    val name1: String = args("name1")
    val name2: String = args("name2")

    val queryResults1: Map[String, QueryResult] = runDataManager.get(name1).queryResults
    val queryResults2: Map[String, QueryResult] = runDataManager.get(name2).queryResults
    val allKeys = (queryResults1.keySet ++ queryResults2.keySet).toArray.sorted

    allKeys.foreach {
      key =>
        logger.info(s"Query $key")
        (queryResults1.get(key), queryResults2.get(key)) match {
          case (None, None) => logger.info("  No result found in run 1 and 2.")
          case (None, Some(_)) => logger.info("  No result found in run 1.")
          case (Some(_), None) => logger.info("  No result found in run 2.")
          case (Some(queryResult1), Some(queryResult2)) =>
            logger.info(s"  Elapsed time matches               : ${queryResult1.elapsedTime == queryResult2.elapsedTime}")
            queryResult1.statementResults.zip(queryResult2.statementResults).foreach {
              case (sr1, sr2) =>
                logger.info(s"  Statement '${sr1.statement.id}' and '${sr2.statement.id}'")
                logger.info(s"    Description matches              : ${sr1.description == sr2.description}")
                logger.info(s"    Details matches                  : ${sr1.details == sr2.details}")
                logger.info(s"    Physical plan description matches: ${sr1.physicalPlanDescription == sr2.physicalPlanDescription}")
            }
        }
    }
  }
}

