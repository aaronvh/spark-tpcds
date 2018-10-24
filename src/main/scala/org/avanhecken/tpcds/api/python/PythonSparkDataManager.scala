package org.avanhecken.tpcds.api.python

import org.avanhecken.tpcds.dataManager.SparkDataManager

class PythonSparkDataManager(database: String) extends SparkDataManager(Map("database" -> database))
