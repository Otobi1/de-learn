#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
        .appName('test') \
        .config("spark.executor.memory", "4g") \
    	.config("spark.driver.memory", "4g") \
        .getOrCreate()

df_green = spark.read.parquet(input_green)

df_yellow = spark.read.parquet(input_yellow)


set(df_green.columns) & set(df_yellow.columns)

df_green = df_green \
                .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') 

df_yellow = df_yellow \
                .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') 


common_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


common_columns = ['VendorID',
                 'dropoff_datetime',
                 'store_and_fwd_flag',
                 'RatecodeID',
                 'PULocationID',
                 'DOLocationID',
                 'passenger_count',
                 'trip_distance',
                 'fare_amount',
                 'extra',
                 'mta_tax',
                 'tip_amount',
                 'tolls_amount',
                 'improvement_surcharge',
                 'total_amount',
                 'payment_type',
                 'congestion_surcharge']



df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))


df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)


df_trips_data.createOrReplaceTempView ('trips_data')


df_result = \
    spark.sql(""" 
                select  
                    PULocationID as revenue_zone,
                    date_trunc('month', 'pickup_datetime')  as revenue_month,
                    service_type, 
                    sum(fare_amount) as revenue_monthly_fare,
                    sum(extra) as revenue_monthly_extra,
                    sum(mta_tax) as revenue_monthly_mta_tax,
                    sum(tip_amount) as revenue_monthly_tip_amount,
                    sum(tolls_amount) as revenue_monthly_tolls_amount,
                    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
                    sum(total_amount) as revenue_monthly_total_amount,
                    avg(passenger_count) as avg_monthly_passenger_count,
                    avg(trip_distance) as avg_monthly_trip_distance
                from trips_data
                group by 1,2,3
                ;
                """)


df_result.coalesce(1).write.parquet(output, mode='overwrite')

