package org.avanhecken.tpcds.run

import org.avanhecken.tpcds.query.QueryResult

case class RunResult(run: Run, queryResults: Map[Short, QueryResult]) {
  def elapsedTime: Long = queryResults.values.map(_.elapsedTime).sum
}


