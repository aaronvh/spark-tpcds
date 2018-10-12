package org.avanhecken.tpcds.dataManager

import org.avanhecken.tpcds.{SparkFlatSpec, SparkTPCDS}
import org.scalatest.Matchers
import org.apache.spark.sql.functions.sum

class SparkDataManagerTest extends SparkFlatSpec with Matchers {
  "getDf" should "give back a dataframe for a specific run" in {
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", resourceLocation, "1"))

    val df = dataManager.getDF("test")
    df.show(false)

    import spark.implicits._
    df
      .groupBy('run)
      .pivot("query")
      .agg(sum('elapsed_time))
      .show(false)
  }
}
