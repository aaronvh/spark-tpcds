package org.avanhecken.tpcds.run

import org.avanhecken.tpcds.statement.StatementResult

trait RunDataManager {
  /**
    *  Check if the name of the run does not correspond to an already existing one.
    *
    * @param name
    * @return true if the run name exists
    */
  def exists(name: String): Boolean

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
  def getNames(): Array[String]
}
