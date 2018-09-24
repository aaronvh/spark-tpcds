package org.avanhecken.tpcds.query

package object query {
  type QueryClass = Int

  final val UNKNOWN: QueryClass = 0
  final val REPORTING: QueryClass = 1
  final val AD_HOC: QueryClass = 2
  final val ITERATIVE_OLAP: QueryClass = 3
  final val DATA_MINING: QueryClass = 4

  /*
  Reporting Class 41
  Ad Hoc Class 59
  Iterative Class 4
  Data Mining Class 23
  */
}
