from pyspark.sql import SQLContext
from pyspark.sql import DataFrame


class SparkDataManager:
    def __init__(self, spark, database):
        self.sqlctx = SQLContext(spark.sparkContext)
        self._data_manager = spark.sparkContext._jvm.org.avanhecken.tpcds.api.python.PythonSparkDataManager(database)

    def get_df(self, name):
        return DataFrame(self._data_manager.getDF(name), self.sqlctx)
