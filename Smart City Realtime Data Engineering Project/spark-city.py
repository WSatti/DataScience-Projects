from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

from config import configuration


def main():
    spark = SparkSession.builder \
        .appName('SmartCity') \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.jars.packages", "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    vehicleSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True),
    ])

    gpsSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicleType', StringType(), True),
    ])

    trafficSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('cameraId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('snapshot', DoubleType(), True),
    ])

    weatherSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('weatherCondition', StringType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('windSpeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('incidentId', StringType(), True),
        StructField('type', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('status', StringType(), True),
        StructField('description', StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '5 minutes'))

    def streamWriter(input_df, checkpoint_folder, output_path):
        return (input_df.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpoint_folder)
                .option('path', output_path)
                .outputMode('append')
                .start())

    vehicle_df = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergency_df = read_kafka_topic('emer_data', emergencySchema).alias('emergency')

    query1 = streamWriter(vehicle_df, checkpoint_folder='s3a://data-engineering-project-aws/checkpoints/vehicle_data',
                          output_path='s3a://data-engineering-project-aws/data/vehicle_data')
    query2 = streamWriter(gps_df, checkpoint_folder='s3a://data-engineering-project-aws/checkpoints/gps_data',
                          output_path='s3a://data-engineering-project-aws/data/gps_data')
    query3 = streamWriter(traffic_df, checkpoint_folder='s3a://data-engineering-project-aws/checkpoints/traffic_data',
                          output_path='s3a://data-engineering-project-aws/data/traffic_data')
    query4 = streamWriter(weather_df, checkpoint_folder='s3a://data-engineering-project-aws/checkpoints/weather_data',
                          output_path='s3a://data-engineering-project-aws/data/weather_data')
    query5 = streamWriter(emergency_df, checkpoint_folder='s3a://data-engineering-project-aws/checkpoints/emer_data',
                          output_path='s3a://data-engineering-project-aws/data/emer_data')

    query5.awaitTermination()


if __name__ == '__main__':
    main()
