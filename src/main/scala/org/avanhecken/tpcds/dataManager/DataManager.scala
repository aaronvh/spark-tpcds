package org.avanhecken.tpcds.dataManager

import org.avanhecken.tpcds.run.{Run, RunResult}
import org.avanhecken.tpcds.statement.StatementResult

trait DataManager extends Serializable {
  /**
    * Save the run if it does not exists.
    *
    * @param statementResult
    */
  def save(run: Run): Unit

  /**
    * Save the statement result.
    *
    * @param statementResult
    */
  def save(statementResult: StatementResult): Unit

  /**
    * Retrieve the results of the run with this name.
    *
    * @param name
    * @return the result of the run corresponding to this name
    */
  def get(name: String): RunResult

  /**
    * Collect the names of all the runs.
    *
    * @return the names of all the runs
    */
  def getNames: Array[String]
}
