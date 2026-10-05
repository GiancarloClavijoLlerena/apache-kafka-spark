from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("KafkaStreamingActividad").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

mensajes = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "actividad-topic")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) AS mensaje")
)

consulta = (
    mensajes.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

# Run briefly so the script demonstrates streaming without remaining blocked.
consulta.awaitTermination(30)
consulta.stop()
spark.stop()
