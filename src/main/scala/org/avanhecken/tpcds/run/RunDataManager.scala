package org.avanhecken.tpcds.run

import scala.org.avanhecken.tpcds.ArgumentParser.Args

trait RunDataManager {
  val args: Args

  /**
    *  Check if the run does not correspond to an already existing one.
    *
    * @param name
    * @return
    */
  def exists(run: Run): Boolean

  /**
    * Save the run if it does not exists.
    *
    * @param run
    */
  def save(runResult: RunResult): Unit
}
