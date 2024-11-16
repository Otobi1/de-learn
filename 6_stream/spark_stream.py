import findspark
findspark.init()

import logging
import os
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    logging.info("Table created successfully!")


def insert_data(session, **kwargs):
    logging.info("Inserting data...")

    try:
        user_id = uuid.UUID(kwargs.get('id'))
        first_name = kwargs.get('first_name')
        last_name = kwargs.get('last_name')
        gender = kwargs.get('gender')
        address = kwargs.get('address')
        postcode = kwargs.get('post_code')
        email = kwargs.get('email')
        username = kwargs.get('username')
        dob = kwargs.get('dob')
        registered_date = kwargs.get('registered_date')
        phone = kwargs.get('phone')
        picture = kwargs.get('picture')

        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except ValueError as ve:
        logging.error(f"Invalid UUID format for id: {ve}")
    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")


def create_spark_connection() -> object:
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("DEBUG")  # Set to DEBUG for detailed logs
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to: {e}", exc_info=True)
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.error(f"Kafka dataframe could not be created due to: {e}", exc_info=True)
    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}", exc_info=True)
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("Selection dataframe created successfully!")
    return sel


if __name__ == "__main__":
    # Ensure checkpoint directory exists
    checkpoint_dir = '/tmp/checkpoint'
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir)

    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                loggingogging.info("Streaming is being started...")
                try:
                    streaming_query = (selection_df.writeStream
                                       .format("org.apache.spark.sql.cassandra")
                                       .option('checkpointLocation', checkpoint_dir)
                                       .option('keyspace', 'spark_streams')
                                       .option('table', 'created_users')
                                       .start())

                    streaming_query.awaitTermination()
                except Exception as e:
                    logging.error(f"Streaming failed due to: {e}", exc_info=True)
