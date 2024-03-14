from pyspark.sql import SparkSession


class JobSparkSessionManager:
    def __init__(self, appname, log_level=None):
        self.appname = appname
        self.spark_session = None
        self.log_level = log_level

    def get_spark_session(self):
        if self.spark_session is None:
            self.spark_session = SparkSession.builder.appName(
                self.appname
            ).getOrCreate()

            if self.log_level is not None:
                self.spark_session.sparkContext.setLogLevel(self.log_level)

        return self.spark_session

    def stop_spark_session(self):
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None

    def start(self):
        self.get_spark_session()

    def stop(self):
        self.stop_spark_session()
