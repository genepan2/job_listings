from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(self, appname):
        self.appname = appname
        self.spark_session = None

    def get_spark_session(self):
        if self.spark_session is None:
            self.spark_session = SparkSession.builder.appName(
                self.appname).getOrCreate()
        return self.spark_session

    def stop_spark_session(self):
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None

    def start(self):
        self.get_spark_session()

    def stop(self):
        self.stop_spark_session()
