types.StructField([
    types.StructField('hvfhs_license_num', types.StringType(), types.True), 
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.Integer(), True), 
    types.StructField('DOLocationID', types.Integer(), True), 
    types.StructField('SR_Flag', types.StringType(), True)
    ])


 
# green 

types.StructType([
    types.StructField('VendorID', types.IntegerType(), True), 
    types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('RatecodeID', types.IntegerType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('passenger_count', types.IntegerType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('ehail_fee', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.DoubleType(), True), 
    types.StructField('payment_type', types.IntegerType(), True), 
    types.StructField('trip_type', types.IntegerType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True)
    ])


# yellow 

types.StructType([
    types.StructField('VendorID', types.IntegerType(), True), 
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
    types.StructField('passenger_count', types.IntegerType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('RatecodeID', types.IntegerType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('payment_type', types.IntegerType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.DoubleType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True)
    ])



select  
    pickup_zone as revenue_zone,
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

types.StructType([
    types.StructField('hour', TimestampType(), True), 
    types.StructField('zone', IntegerType(), True), 
    types.StructField('revenue', DoubleType(), True), 
    types.StructField('count', IntegerType(), True)
    ])

python 10_local_spark_cluster.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020

URL="spark://de-zoomcamp.europe-west1-b.c.tonal-works-431719-h7.internal:7077"

spark-submit \
    --master="${URL}" \
    10_local_spark_cluster.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021


--input_green=gs://de_zoomcamp_nytaxi/pq/green/2021/*/
--input_yellow=gs://de_zoomcamp_nytaxi/pq/yellow/2021/*/
--output=gs://de_zoomcamp_nytaxi/report-2021



{
  "reference": {
    "jobId": "job-7966ec34",
    "projectId": "tonal-works-431719-h7"
  },
  "placement": {
    "clusterName": "de-zoomcamp-cluster"
  },
  "status": {
    "state": "DONE",
    "stateStartTime": "2024-09-15T18:06:14.270010Z"
  },
  "yarnApplications": [
    {
      "name": "test",
      "state": "FINISHED",
      "progress": 1,
      "trackingUrl": "http://de-zoomcamp-cluster-m.europe-west6-b.c.tonal-works-431719-h7.internal.:8088/proxy/application_1726420389483_0001/"
    }
  ],
  "statusHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2024-09-15T18:05:22.070525Z"
    },
    {
      "state": "SETUP_DONE",
      "stateStartTime": "2024-09-15T18:05:22.116785Z"
    },
    {
      "state": "RUNNING",
      "details": "Agent reported job success",
      "stateStartTime": "2024-09-15T18:05:22.411387Z"
    }
  ],
  "driverControlFilesUri": "gs://dataproc-staging-europe-west6-869527020226-yjvdo4qn/google-cloud-dataproc-metainfo/ac69dfa3-5fc9-4ab9-8abe-afa408fc7b52/jobs/job-7966ec34/",
  "driverOutputResourceUri": "gs://dataproc-staging-europe-west6-869527020226-yjvdo4qn/google-cloud-dataproc-metainfo/ac69dfa3-5fc9-4ab9-8abe-afa408fc7b52/jobs/job-7966ec34/driveroutput",
  "jobUuid": "28d883df-b30d-4835-8bba-3edf57b76db5",
  "done": true,
  "pysparkJob": {
    "mainPythonFileUri": "gs://de_zoomcamp_nytaxi/code/10_local_spark_cluster.py",
    "args": [
      "--input_green=gs://de_zoomcamp_nytaxi/pq/green/2021/*/",
      "--input_yellow=gs://de_zoomcamp_nytaxi/pq/yellow/2021/*/",
      "--output=gs://de_zoomcamp_nytaxi/report-2021"
    ]
  }
}


de-zoomcamp-cluster
gs://de_zoomcamp_nytaxi/code/10_local_spark_cluster.py
"--input_green=gs://de_zoomcamp_nytaxi/pq/green/2021/*/",
"--input_yellow=gs://de_zoomcamp_nytaxi/pq/yellow/2021/*/",
"--output=gs://de_zoomcamp_nytaxi/report-2021"
  
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    gs://de_zoomcamp_nytaxi/code/10_local_spark_cluster.py \
    -- \
        --input_green=gs://de_zoomcamp_nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://de_zoomcamp_nytaxi/pq/yellow/2020/*/ \
        --output=gs://de_zoomcamp_nytaxi/report-2020




gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    gs://de_zoomcamp_nytaxi/code/11_spark_sql_bigquery.py \
    -- \
        --input_green=gs://de_zoomcamp_nytaxi/pq/green/2021/*/ \
        --input_yellow=gs://de_zoomcamp_nytaxi/pq/yellow/2021/*/ \
        --output=trips_data_all.reports-2021


trips_data_all.reports-2020

