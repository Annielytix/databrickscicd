# MAGIC %md
# MAGIC #Streaming Mobile Game Events
# MAGIC 
# MAGIC This demo will simulate an end-to-end ETL pipeline for mobile game events
# MAGIC 
# MAGIC #####Steps
# MAGIC - <a href="https://docs.databricks.com/_static/notebooks/mobile-event-generator.html" target="_new">Event Generator</a> that sends events to REST end point (Amazon API Gateway)
# MAGIC - API Gateway Triggers an AWS Lambda function that writes data to Kinesis stream
# MAGIC - Ingest from a Kinesis stream in real time into a Databricks Delta table
# MAGIC - Perform necessary transformations to extract real-time KPI's
# MAGIC - Visualize KPI's on a dashboard
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2018/06/Mobile-Gaming-Events-Data-Pipeline.png" width="960"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup Instructions
# MAGIC - Start cluster with IAM role "syu_mobile_streaming_demo_role"
# MAGIC - Start <a href="https://docs.databricks.com/_static/notebooks/mobile-event-generator.html" target="_new">Mobile ETL Event Generator</a>
# MAGIC - Clear out target Databricks Delta table using parallelized reset code

# COMMAND ----------

# MAGIC %md
# MAGIC #####Setup Kinesis Stream Reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

kinesisDataFrame = spark \
.readStream \
.format('kinesis') \
.option('streamName','<YOUR_STREAM_NAME>') \
.option('initialPosition','earliest') \
.option('region','us-west-2') \
.load()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Defining Incoming Data Schemas

# COMMAND ----------

kinesisSchema = StructType() \
            .add('body', StringType()) \
            .add('resource', StringType()) \
            .add('requestContext',StringType()) \
            .add('queryStringParameters', StringType()) \
            .add('httpMethod', StringType()) \
            .add('pathParameters', StringType()) \
            .add('headers', StringType()) \
            .add('stageVariables', StringType()) \
            .add('path', StringType()) \
            .add('isBase64Encoded', StringType())

eventSchema = StructType().add('eventName', StringType()) \
              .add('eventTime', TimestampType()) \
              .add('eventParams', StructType() \
                   .add('game_keyword', StringType()) \
                   .add('app_name', StringType()) \
                   .add('scoreAdjustment', IntegerType()) \
                   .add('platform', StringType()) \
                   .add('app_version', StringType()) \
                   .add('device_id', StringType()) \
                   .add('client_event_time', TimestampType()) \
                   .add('amount', DoubleType())
                  )            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Code to reset environment

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE mobile_events_delta_raw 
# MAGIC USING DELTA 
# MAGIC location '/path/to/mobile_events_stream/';

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.parallelize(dbutils.fs.ls("/path/to/mobile_events_stream/").map(_.path)).foreach{
# MAGIC   dbutils.fs.rm(_, true)
# MAGIC }

# COMMAND ----------

# MAGIC %sql REFRESH TABLE mobile_events_delta_raw

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read incoming stream into Databricks Delta table

# COMMAND ----------

gamingEventDF = kinesisDataFrame.selectExpr("cast (data as STRING) jsonData") \
.select(from_json('jsonData',kinesisSchema).alias('requestBody'))\
.select(from_json('requestBody.body', eventSchema).alias('body'))\
.select('body.eventName', 'body.eventTime', 'body.eventParams')

# COMMAND ----------

base_path = '/path/to/mobile_events_stream/'

# COMMAND ----------

eventsStream = gamingEventDF.filter(gamingEventDF.eventTime.isNotNull()).withColumn("eventDate", to_date(gamingEventDF.eventTime)) \
  .writeStream \
  .partitionBy('eventDate') \
  .format('delta') \
  .option('checkpointLocation', base_path + '/_checkpoint') \
  .start(base_path)

# COMMAND ----------

# MAGIC %sql SELECT eventName, count(*) FROM mobile_events_delta_raw GROUP BY 1 ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Analytics, KPI's, Oh My!

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Events in the last hour?

# COMMAND ----------

countsDF = gamingEventDF.withWatermark("eventTime", "180 minutes").groupBy(window("eventTime", "60 minute")).count()
countsQuery = countsDF.writeStream \
  .format('memory') \
  .queryName('incoming_events_counts') \
  .start()

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bookings in the last hour?

# COMMAND ----------

bookingsDF = gamingEventDF.withWatermark("eventTime", "180 minutes").filter(gamingEventDF.eventName == 'purchaseEvent').groupBy(window("eventTime", "60 minute")).sum("eventParams.amount")
bookingsQuery = bookingsDF.writeStream \
  .format('memory') \
  .queryName('incoming_events_bookings') \
  .start()

# COMMAND ----------

display(bookingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How about DAU (Daily Active Users)?

# COMMAND ----------

# MAGIC %sql select count (distinct eventParams.device_id) as DAU from mobile_events_delta_raw where to_date(eventTime) = current_date

# COMMAND ----------

# MAGIC %md
# MAGIC #####OK, how about the housekeeping?
# MAGIC 
# MAGIC A common problem with streaming is that you end up with a lot of small files.  If only there were a way to perform compaction...

# COMMAND ----------

# For the sake of example, let's stop the streams to demonstrate compaction and vacuum
eventsStream.stop()


# COMMAND ----------

# MAGIC %fs ls /path/to/mobile_events_stream/eventDate=2018-02-15/

# COMMAND ----------

# MAGIC %md
# MAGIC ...and here it is!

# COMMAND ----------

# MAGIC %sql OPTIMIZE '/path/to/mobile_events_stream/'

# COMMAND ----------

# MAGIC %fs ls /path/to/mobile_events_stream/eventDate=2018-02-15/

# COMMAND ----------

# MAGIC %md
# MAGIC #####Great, now how about cleaning up those old files?
# MAGIC NOTE: Do not use a retention of 0 hours in production, as this may affect queries that are currently in flight.  By default this value is 7 days.  We're using 0 hours here for purposes of demonstration only.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM '/path/to/mobile_events_stream/' RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %fs ls /path/to/mobile_events_stream/eventDate=2018-02-15/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Where can I learn more?
# MAGIC 
# MAGIC * [Databricks](http://www.databricks.com/)
# MAGIC * [Databricks Delta](http://docs.databricks.com/delta/index.html)
# MAGIC * [Databricks Guide > Structured Streaming](https://docs.databricks.com/spark/latest/structured-streaming/index.html)