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
    *  Check if the run does not correspond to an already existing one.
    *
    * @param run
    * @return true if the run exists
    */
  def exists(run: Run): Boolean

  /**
    * Save the run if it does not exists.
    *
    * @param run
    */
  def save(runResult: RunResult): Unit
}
