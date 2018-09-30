package org.avanhecken.tpcds.run

import org.avanhecken.tpcds.ArgumentParser.Args

trait RunDataManager {
  val args: Args

  /**
    * Check if the run table exist and if the schema is correct.
    *
    * @return the initialized Run DataManager
    */
  def initialize(): RunDataManager

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
    * @param run
    */
  def save(runResult: RunResult): Unit

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
