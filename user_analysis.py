from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, datediff, current_date, concat_ws, explode, sha2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from cassandra.cluster import Cluster

# Spark configuration 
spark = SparkSession.builder \
    .appName("UserProfilesPipeline") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


# Kafka source configuration
kafka_source = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_profiles") \
    .option("failOnDataLoss", "false") \
    .load()

# the schema for parsing JSON data
schema =  StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("gender", StringType(), True),
            StructField("nat", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("street", StructType([
                    StructField("number", IntegerType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", IntegerType(), True),
                StructField("coordinates", StructType([
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True)
                ]), True),
            ]), True),
            StructField("email", StringType(), True),
            StructField("login", StructType([
                StructField("uuid", StringType(), True),
                StructField("username", StringType(), True),
                StructField("password", StringType(), True)
            ]), True),
            StructField("dob", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("phone", StringType(), True),
            StructField("cell", StringType(), True)
        ]), True),
    True)
])


# Parse JSON data
parsedStreamDF = kafka_source.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .select(explode(col("results")).alias("final-results"))



final_output_df = parsedStreamDF.select(
    col("final-results.login.uuid").alias("id"),
    col("final-results.gender").alias("gender"),
    col("final-results.nat").alias("nationality"),
    col("final-results.name.title").alias("title"),
    col("final-results.name.first").alias("first"),
    col("final-results.name.last").alias("last"),
    concat_ws(" ", col("final-results.name.first"), col("final-results.name.last")).alias("full_name"),
    col("final-results.login.username").alias("username"),
    sha2(col("final-results.email"), 256).alias("email"),  
    sha2(col("final-results.phone"), 256).alias("phone"),  
    concat_ws(", ", col("final-results.location.city"), 
            col("final-results.location.state"), 
            col("final-results.location.country")).alias("full_address"),
    (datediff(current_date(), col("final-results.dob.date")) / 365).cast("int").alias("age"),
    col("final-results.registered.date").alias("inscription"),
    col("final-results.dob.date").alias("birthday"),
    col("final-results.location.city").alias("city"),
    col("final-results.location.country").alias("country"),
    col("final-results.location.state").alias("state")
)





mongo_uri = "mongodb://localhost:27017"  
mongo_db_name = "user_profiles"
collection_name = "user_collection"

# Save the data to MongoDB
query = parsedStreamDF.select(col("final-results.email").alias("email"), col("final-results.nat").alias("nationality"),col("final-results.location.country").alias("country"), col("final-results.dob.age").alias("age")) \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db_name) \
        .option("collection", collection_name) \
        .mode("append") \
        .save()
    ) \
    .outputMode("append") \
    .start()






cassandra_host = 'localhost'
cassandra_port = 9042
keyspaceName = 'user_profiles'
tableName = 'user_profiles'


def connect_to_cassandra(host,port):
    try:
        # provide contact points
        cluster = Cluster([host],port=port)
        session = cluster.connect()
        print("Connection established successfully.")
        return session
    except:
        print("Connection failed.")

def create_cassandra_keyspace(session,keyspaceName):
    try:
        create_keyspace_query = """ CREATE KEYSPACE IF NOT EXISTS """+keyspaceName+ \
        """ WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"""
        session.execute(create_keyspace_query)
        print("Keyspace was created successfully.")
    except:
        print(f"Error in creating keyspace {keyspaceName}.")

def create_cassandra_table(session,tableName):
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            id UUID PRIMARY KEY,
            gender TEXT,
            title TEXT,
            full_name TEXT,
            first TEXT,
            last TEXT,
            username TEXT,
            full_address TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            state TEXT,
            nationality TEXT,
            country TEXT,
            birthday TEXT,
            age INT,
            inscription TEXT
        )
        """
        session.execute(create_table_query)
        print("table was created successfully.")
    except:
        print(f"Error in creating table {tableName}.")


session = connect_to_cassandra(cassandra_host,cassandra_port)
create_cassandra_keyspace(session,keyspaceName)
session.set_keyspace(keyspaceName)
create_cassandra_table(session,tableName)

# Save the DataFrame to Cassandra
result_df_clean = final_output_df.filter("id IS NOT NULL")

result_df_clean.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint/omar/data") \
    .option("keyspace", keyspaceName) \
    .option("table", tableName) \
    .start()

query = final_output_df.writeStream.outputMode("append").format("console").start()


# Start the Spark session
spark.streams.awaitAnyTermination()
