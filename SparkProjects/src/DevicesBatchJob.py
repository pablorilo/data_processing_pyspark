from ConnectionEnvironment import *

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, sum
from pyspark.sql.types import TimestampType

from datetime import datetime


class DevicesBatchJob(ConnectionEnvironment):

    def __init__(self):
        ConnectionEnvironment.__init__(self)
        self.spark = SparkSession.builder.master("local[*]").appName("DevicesBatchJob").getOrCreate()

    def readFromStorage(self, storagePath: str, filterDate: datetime) -> DataFrame:
        return (self.spark
                .read
                .format("parquet")
                .load(f'{storagePath}/')
                .filter(col("year") == str(filterDate.year))
                .filter(col("month") == str(filterDate.month))
                .filter(col("day") == str(filterDate.day))
                .filter(col("hour") == str(filterDate.hour)).cache()
                )

    def readUserMetadata(self, jdbcURI: str, jdbcTable: str, user: str, password: str) -> DataFrame:
        return (self.spark
                .read
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", f"jdbc:postgresql://{jdbcURI}:5432/postgres")
                .option("dbtable", jdbcTable)
                .option("user", user)
                .option("password", password)
                .load())

    @staticmethod
    def computeTotalBytesAgg(df: DataFrame, colName: str, metricName: str, filterDate: datetime) -> DataFrame:
        return (df
                .select(col(colName).alias("id"), col("bytes"))
                .groupBy(col("id"))
                .agg(sum(col("bytes")).alias("value"))
                .withColumn("type", lit(metricName))
                .withColumn("timestamp", lit(filterDate.timestamp()).cast(TimestampType()))
                )

    @staticmethod
    def calculateUserQuotaLimit(userTotalBytesDF: DataFrame, userMetadataDF: DataFrame) -> DataFrame:
        return (userTotalBytesDF
                .alias("user")
                .select(col("id"), col("value"), col("timestamp"))
                .join(
                    userMetadataDF.select(col("id"), col("email"), col("quota")).alias("metadata"),
                    (col("user.id") == col("metadata.id")) & (col("user.value") > col("metadata.quota")), "inner"
                ).select(col("metadata.email"), col("user.value").alias("usage"), col("metadata.quota"), col("user.timestamp"))
                )

    @staticmethod
    def writeToJdbc(df: DataFrame, jdbcURI: str, jdbcTable: str, user: str, password: str):
        return (df
                .write
                .mode("append")
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", F"jdbc:postgresql://{jdbcURI}:5432/postgres")
                .option("dbtable", jdbcTable)
                .option("user", user)
                .option("password", password)
                .save())

    @staticmethod
    def writeToStorage(df: DataFrame, storagePath: str):
        return (df
                .write
                .partitionBy("year", "month", "day", "hour")
                .format("parquet")
                .mode("overwrite")
                .save(f"{storagePath}/historical"))

    def run(self):
        now = datetime.now()
        # --Leemos datos del storage para datetime.now() podemos especificar la fecha si queremos
        devicesDF = self.readFromStorage("./proyecto/devices/data", now)

        # --Cargamos datos de la tabla user_metadata de la instancia postgres
        userMetadataDF = self.readUserMetadata(self.jdbcUri, "user_metadata", self.user, self.password)

        # --Creamos los 3 df que corresponden a los bytes totales de cada usuario, antena, y app
        userTotalBytesDF = self.computeTotalBytesAgg(devicesDF, "id", "user_bytes_total", now)
        antennaTotalBytesDF = self.computeTotalBytesAgg(devicesDF, "antenna_id", "antenna_bytes_total", now)
        appTotalBytesDF = self.computeTotalBytesAgg(devicesDF, "app", "app_bytes_total", now)

        # --Calculamos el df de usuarios que han superado la cuota
        userQuotaLimit = self.calculateUserQuotaLimit(userTotalBytesDF, userMetadataDF)

        # --Insertamos en jdbc
        self.writeToJdbc(userTotalBytesDF, self.jdbcUri, "py_bytes_hourly", self.user, self.password)
        self.writeToJdbc(antennaTotalBytesDF, self.jdbcUri, "py_bytes_hourly", self.user, self.password)
        self.writeToJdbc(appTotalBytesDF, self.jdbcUri, "py_bytes_hourly", self.user, self.password)

        self.writeToJdbc(userQuotaLimit, self.jdbcUri, "py_user_quota_limit", self.user, self.password)
