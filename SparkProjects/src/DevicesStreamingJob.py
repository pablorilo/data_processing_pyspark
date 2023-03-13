from ConnectionEnvironment import *

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, lit, sum, window
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

from pyspark.sql.functions import year, month, dayofmonth, hour


class DevicesStreamingJob(ConnectionEnvironment):

    def __init__(self):
        ConnectionEnvironment.__init__(self)
        self.spark = SparkSession.builder.master("local[*]").appName("DevicesBatchJob").getOrCreate()

    @staticmethod
    def bytes_total_agg(data: DataFrame, agg_column: str) -> DataFrame:
        return (data
                .select(col("timestamp"), agg_column, col("bytes"))
                .withWatermark("timestamp", "1 minutes")
                .groupBy(window(col("timestamp"), "5 minutes"), agg_column)
                .agg(sum("bytes").alias("total_bytes")).select(
                    col("window.start").cast("timestamp").alias("timestamp"),
                    col(agg_column).alias("id"),
                    col("total_bytes").alias("value"),
                    lit(f"{agg_column}_total_bytes").alias("type")
                ))

    @staticmethod
    def parse_json_data(data_frame: DataFrame) -> DataFrame:
        json_schema = StructType([
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("id", StringType(), nullable=False),
            StructField("antenna_id", StringType(), nullable=False),
            StructField("bytes", LongType(), nullable=False),
            StructField("app", StringType(), nullable=False)
        ])

        return (data_frame
                .select(from_json(col("value").cast("string"), json_schema).alias("json"))
                .select("json.*")
                )

    def readFromKafka(self, kafkaServer: str, topic: str):
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", topic)
                .load())

    @staticmethod
    def write_to_storage(data_frame: DataFrame, storage_root_path: str):
        (data_frame
         .select(
            col("id"),
            col("antenna_id"),
            col("bytes"),
            col("app"),
            year(col("timestamp")).alias("year"),
            month(col("timestamp")).alias("month"),
            dayofmonth(col("timestamp")).alias("day"),
            hour(col("timestamp")).alias("hour"),
            ).writeStream
             .format("parquet")
             .option("path", f"{storage_root_path}/data")
             .option("checkpointLocation", f"{storage_root_path}/checkpoint")
             .partitionBy("year", "month", "day", "hour")
             .start()).awaitTermination()  # comentar awaitTermination() si quieres que se ejecute en segundo plano

    @staticmethod
    def write_to_jdbc(data_frame: DataFrame, jdbc_uri: str, jdbc_table: str, user: str, password: str):
        (data_frame
         .writeStream
         .foreachBatch(lambda data, batch_id: data
                       .write
                       .mode("append")
                       .format("jdbc")
                       .option("driver", "org.postgresql.Driver")
                       .option("url", f"jdbc:postgresql://{jdbc_uri}:5432/postgres")
                       .option("dbtable", jdbc_table)
                       .option("user", user)
                       .option("password", password)
                       .save())
         .start())

    def run(self):
        # -- Conectamos con la transmision en streaming y aplicamos un esquema a los datos
        kafkaDF = self.readFromKafka(self.kafkaServer, self.topic)
        parsed_df = self.parse_json_data(kafkaDF)

        # -- Agregamos los datos a la columna que proceda agrupados en ventanas de tiempo de 5 minutos
        appStream = self.bytes_total_agg(parsed_df, "app")
        userStream = self.bytes_total_agg(parsed_df.withColumnRenamed("id", "user"), "user")
        antennaStream = self.bytes_total_agg(parsed_df.withColumnRenamed("antenna_id", "antenna"), "antenna")

        # -- Escribimos los datos agrupados y transformados en la instancia de postgress
        self.write_to_jdbc(appStream, self.jdbcUri, self.jdbcTable, self.user, self.password)
        self.write_to_jdbc(userStream, self.jdbcUri, self.jdbcTable, self.user, self.password)
        self.write_to_jdbc(antennaStream, self.jdbcUri, self.jdbcTable, self.user, self.password)

        # Guardamos los datos base en local
        self.write_to_storage(parsed_df, self.storagePath)
