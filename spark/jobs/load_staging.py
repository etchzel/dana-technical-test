import pyspark
import argparse
import os
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

SPARK_HOME = os.environ['SPARK_HOME']

def flatten_schema(schema, prefix=""):
    return_schema = []
    for field in schema.fields:
        if isinstance(field.dataType, pyspark.sql.types.StructType):
            if prefix:
                return_schema = return_schema + flatten_schema(field.dataType, "{}.{}".format(prefix, field.name))
            else:
                return_schema = return_schema + flatten_schema(field.dataType, field.name)
        else:
            if prefix:
                field_path = "{}.{}".format(prefix, field.name)
                return_schema.append(pyspark.sql.functions.col(field_path).alias(field_path.replace(".", "_")))
            else:
                return_schema.append(field.name)
    return return_schema

def main(params):
    # parametrizes db connection config
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    spark = SparkSession.builder \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.5.1.jar") \
        .master("spark://spark-master:7077") \
        .appName("load_staging") \
        .getOrCreate()

    # Read dataset
    print("Reading yelp dataset")
    df_business = spark.read.json(f"{SPARK_HOME}/dataset/yelp/json/yelp_academic_dataset_business.json")
    df_checkin = spark.read.json(f"{SPARK_HOME}/dataset/yelp/json/yelp_academic_dataset_checkin.json")
    df_review = spark.read.json(f"{SPARK_HOME}/dataset/yelp/json/yelp_academic_dataset_review.json")
    df_tip = spark.read.json(f"{SPARK_HOME}/dataset/yelp/json/yelp_academic_dataset_tip.json")
    df_user = spark.read.json(f"{SPARK_HOME}/dataset/yelp/json/yelp_academic_dataset_user.json")
    print("Finish reading yelp dataset")

    # Flatten data with nested structure
    print("Flatten nested data")
    df_business = df_business.select(flatten_schema(df_business.schema))
    print("Finish flattening data")

    # Write to csv
    # print("Converting to CSV")
    # df_business.write.mode('ignore').options(header=True, delimiter=";", compression="gzip").csv(f"{SPARK_HOME}/dataset/yelp/csv/yelp_dataset_business")
    # df_checkin.write.mode('ignore').options(header=True, delimiter=";", compression="gzip").csv(f"{SPARK_HOME}/dataset/yelp/csv/yelp_dataset_checkin")
    # df_review.write.mode('ignore').options(header=True, delimiter=";", compression="gzip").csv(f"{SPARK_HOME}/dataset/yelp/csv/yelp_dataset_review")
    # df_tip.write.mode('ignore').options(header=True, delimiter=";", compression="gzip").csv(f"{SPARK_HOME}/dataset/yelp/csv/yelp_dataset_tip")
    # df_user.write.mode('ignore').options(header=True, delimiter=";", compression="gzip").csv(f"{SPARK_HOME}/dataset/yelp/csv/yelp_dataset_user")
    # print("Finish converting to csv")

    # # create schema layers
    # db_uri = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    # engine = create_engine(db_uri)

    # print("Creating schema layer")
    # with engine.connect() as conn:
    #     conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    #     conn.execute("CREATE SCHEMA IF NOT EXISTS clean;")
    #     conn.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
    # print("Created schema layer for the data warehouse")

    # Load to postgres staging layer
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": f"{user}",
        "password": f"{password}",
        "driver": "org.postgresql.Driver"
    }

    print("Writing data to staging layer")
    df_business.write.mode('overwrite').jdbc(jdbc_url, table="staging.yelp_business", properties=properties)
    df_checkin.write.mode('overwrite').jdbc(jdbc_url, table="staging.yelp_checkin", properties=properties)
    df_review.write.mode('overwrite').jdbc(jdbc_url, table="staging.yelp_review", properties=properties)
    df_tip.write.mode('overwrite').jdbc(jdbc_url, table="staging.yelp_tip", properties=properties)
    df_user.write.mode('overwrite').jdbc(jdbc_url, table="staging.yelp_user", properties=properties)
    print("Finish writing to staging")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')

    args = parser.parse_args()

    main(args)