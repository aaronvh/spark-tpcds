package org.avanhecken.tpcds

import org.scalatest.Matchers

class SparkTPCDSTest extends SparkFlatSpec with Matchers {
  "execute" should "run first query successfully" in {
    SparkTPCDS.main(Array("execute", "test", "Test if query 1 runs successfully", "tpcds", resourceLocation, "1"))

    spark.table(runsTable).show(false)
  }

  "list" should "print out the names of all runs" in {
    SparkTPCDS.main(Array("execute", "test1", "Test1 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test2", "Test2 if query 1 runs successfully", "tpcds", resourceLocation, "2"))
    SparkTPCDS.main(Array("list", "tpcds"))
  }

  "compare" should "print out the comparison between two equal runs" in {
    SparkTPCDS.main(Array("execute", "test3", "Test3 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test4", "Test4 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("compare", "tpcds", "test3", "test4"))
  }

  it should "print out the comparison between two non-equal runs" in {
    SparkTPCDS.main(Array("execute", "test5", "Test5 if query 1 runs successfully", "tpcds", resourceLocation, "1"))
    SparkTPCDS.main(Array("execute", "test6", "Test6 if query 1 runs successfully", "tpcds", resourceLocation, "2"))
    SparkTPCDS.main(Array("compare", "tpcds", "test5", "test6"))
  }
}
