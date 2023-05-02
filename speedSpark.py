# Import necessary libraries
import mysql.connector
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()
sc=spark.sparkContext
ssc=StreamingContext(sc,20)


# Define schema for the incoming JSON data
schema = StructType([
    StructField("iss_position", StructType([
        StructField("latitude", StringType()),
        StructField("longitude", StringType())
    ])),
    StructField("message", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("traceback", StringType()),
    StructField("iss_speed", DoubleType()),
    StructField("iss_distance", DoubleType()),
    StructField("iss_time_elapsed", DoubleType())
])

# Read messages from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "speed") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from binary to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and extract the fields
parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .select("iss_position.latitude", "iss_position.longitude", "timestamp","message","traceback","iss_speed","iss_distance", "iss_time_elapsed")


# Define the function to insert data into the database
def write_to_mysql(row):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password123#@!",
        database="spaceStation"
    )

    # Create cursor object
    cursor = mydb.cursor()

    # Define the SQL query to insert data into the table
    query = "INSERT INTO speed_table(latitude, longitude, timestamp, message, traceback, iss_speed, iss_distance, iss_time_elapsed) \
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    # Execute the SQL query with the row data
    cursor.execute(query, (row.latitude, row.longitude, row.timestamp, row.message, row.traceback, row.iss_speed, row.iss_distance, row.iss_time_elapsed))

    # Commit the changes and close the connection
    mydb.commit()
    mydb.close()


# Write the data to the MySQL database using foreach method
parsed_df.writeStream.outputMode("append").format("console").foreach(write_to_mysql).start().awaitTermination() 

