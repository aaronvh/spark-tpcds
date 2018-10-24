class SparkDataManager:
    def __init__(self, spark, database):
        self._data_manager = spark.sparkContext._jvm.org.avanhecken.tpcds.api.python.PythonSparkDataManager(database)

    def get_df(self, name):
        return self._data_manager.getDF(name)
