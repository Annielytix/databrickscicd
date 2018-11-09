// MAGIC %md 
// MAGIC # Real-time Analytics Against Streaming + Historical Data
// MAGIC 
// MAGIC ### Overview
// MAGIC In digital advertising, one of the most important things to be able to deliver to clients is information about how their advertising spend drove results -- and the more quickly we can provide this to clients, the better. To tie conversions to the impressions served in an advertising campaign, companies must perform attribution. Attribution can be a fairly expensive process, and running attribution against constantly updating datasets is challenging without the right technology.
// MAGIC 
// MAGIC Fortunately, Databricks makes this easy with Structured Streaming and Delta.
// MAGIC 
// MAGIC 
// MAGIC ![stream](https://i.imgur.com/FIbTmAE.png)

// COMMAND ----------

/*

Define the following variables (best practice is to use Databricks Secrets):
- awsAccessKeyId
- awsSecreyKey
- kinesisRegion
- kinesisStreamName

You can also use AWS IAM Roles in place of keys.

*/

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName)
  .option("region", kinesisRegion)
  .option("initialPosition", "latest")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()

val kinesisConv = spark.readStream
  .format("kinesis")
  .option("streamName", "adtech-conv")
  .option("region", kinesisRegion)
  .option("initialPosition", "latest")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()

val schemaImp = StructType(Seq( 
  StructField("uid", StringType, true),
  StructField("impTimestamp", TimestampType, true),
  StructField("exchangeID", IntegerType, true),  
  StructField("publisher", StringType, true),
  StructField("creativeID", IntegerType, true),
  StructField("click", StringType, true),
  StructField("advertiserID", IntegerType, true),
  StructField("browser", StringType, true),
  StructField("geo", StringType, true),
  StructField("bidAmount", DoubleType, true)
))

val schemaConv = StructType(Seq(
  StructField("uid", StringType, true),
  StructField("convTimestamp", TimestampType, true), 
  StructField("conversionID", IntegerType, true),
  StructField("advertiserID", IntegerType, true),
  StructField("pixelID", IntegerType, true),
  StructField("conversionValue", DoubleType, true)
))

// VALIDATE AND PROCESS
val imp = kinesis.select(from_json('data.cast("string"), schemaImp) as "fields").select($"fields.*").withColumn("date",$"impTimestamp".cast(DateType))
val con = kinesisConv.select(from_json('data.cast("string"), schemaConv) as "fields").select($"fields.*")

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

// PERSIST IMPRESSION STREAM TO DELTA
imp.withWatermark("impTimestamp", "1 minute")
  .repartition(1)
  .writeStream
  .format("delta")
  .option("path", "tmp/pathto/impressions") // Replace with your desired destination for persisting the impression data; can already contain historical data as long as the schemas match
  .option("checkpointLocation","/tmp/imp_checkpoint_folder")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")).start()

// PERSIST CONVERSION STREAM TO DELTA
con.withWatermark("convTimestamp", "1 minute")
  .repartition(1)
  .writeStream
  .format("delta")
  .option("path", "tmp/pathto/conversions") // Replace with your desired destination for persisting the conversion data
  .option("checkpointLocation","/tmp/conv_checkpoint_folder")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")).start()

// COMMAND ----------

// MAGIC %sql create table attribution.imps using delta location 'dbfs:/tmp/pathto/impressions';
// MAGIC create table attribution.convs using delta location 'dbfs:/tmp/pathto/conversions';

// COMMAND ----------

// MAGIC %md ## Let's see the data update!

// COMMAND ----------

// MAGIC %sql select count(*) from attribution.imps

// COMMAND ----------

// MAGIC %sql select count(*) from attribution.imps

// COMMAND ----------

// MAGIC %sql select count(*) from attribution.convs

// COMMAND ----------

// MAGIC %sql select * from attribution.convs order by convTimestamp desc limit 10

// COMMAND ----------

// MAGIC %md ## Last-Touch Attribution on View

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

val imps = spark.sql("select uid as impUid, advertiserID as impAdv, * from attribution.imps").drop("advertiserID")
val convs = spark.sql("select * from attribution.convs")
val windowSpec = Window.partitionBy("impUid", "impAdv").orderBy(desc("impTimestamp"))
val windowedAttribution = convs.join(imps, imps.col("impUid") === convs.col("uid") && imps.col("impTimestamp") < convs.col("convTimestamp") && imps.col("impAdv") === convs.col("advertiserID")).withColumn("dense_rank", dense_rank().over(windowSpec)).filter($"dense_rank"===1)
windowedAttribution.createOrReplaceTempView("realTimeAttribution")

// COMMAND ----------

// MAGIC %sql select * from realTimeAttribution order by convTimestamp desc

// COMMAND ----------

// MAGIC %md ## Last-Touch Attribution on Stream

// COMMAND ----------

val imps = sql("select uid as impUid, advertiserID as impAdv, * from attribution.imps").drop("advertiserID")

val windowSpec = Window.partitionBy("impUid", "impAdv").orderBy(desc("impTimestamp"))
val attr = con.join(imps, imps.col("impUid") === con.col("uid") && imps.col("impTimestamp") < con.col("convTimestamp") && imps.col("impAdv") === con.col("advertiserID"))

display(attr)

// COMMAND ----------

// MAGIC %md ## Weighted Attribution on View

// COMMAND ----------

val attrWindow = Window
.partitionBy("uid")
.orderBy($"impTimestamp".desc)
val attrRank = dense_rank().over(attrWindow)

val rankedWindow = Window
.partitionBy("conversionID")
val numAttrImps = max(col("attrRank")).over(rankedWindow)

// Reference impression and conversion table
val imps = spark.sql("select * from attribution.imps").withColumn("date", $"impTimestamp".cast(DateType)).drop("advertiserID")
val convs = spark.sql("select * from attribution.convs").withColumnRenamed("uid","cuid")
val joined = imps.join(convs, imps.col("uid") === convs.col("cuid") && imps.col("impTimestamp") < convs.col("convTimestamp")).drop("cuid")

val attributed = joined.withColumn("attrRank",attrRank).withColumn("numImps",numAttrImps).withColumn("weightedRevAttr",$"conversionValue"/$"numImps")

/**/
attributed.createOrReplaceTempView("attributed") 
display(spark.sql("select sum(weightedRevAttr), advertiserID from attributed group by advertiserID"))


// COMMAND ----------

display(attributed)

// COMMAND ----------

// MAGIC %md ### ZOrdering
// MAGIC 
// MAGIC Delta supports optimized data indexing via ZOrdering. ZOrdering allows you to cluster data according to frequently used predicates.
// MAGIC 
// MAGIC For example:
// MAGIC `OPTIMIZE attribution.imps zorder by uid, impTimestamp`
// MAGIC would reorganize the impression data so that all impressions for a given user at a given time are likely colocated in the same file, or same few files (no need to scan full data set.)
// MAGIC 
// MAGIC See our docs for more details.

// COMMAND ----------

// MAGIC %md ## Upsert Support for Corrected or Late Arriving Data
// MAGIC - Users can append a late impression and when attribution view is regenerated, corrected attribution will be reflected.
// MAGIC - Users can overwrite previous records at file-level -- this change will flow into attributed view.
// MAGIC 
// MAGIC Example Code:
// MAGIC 
// MAGIC ~~~~
// MAGIC MERGE INTO attribution.convs AS target
// MAGIC USING updatedConversions AS new
// MAGIC ON  new.uid == target.uid AND new.conversionValue == target.conversionValue 
// MAGIC WHEN matched THEN
// MAGIC UPDATE SET target.uid = new.uid, convTimestamp = current_timestamp(), pixelID = 000000
// MAGIC WHEN NOT MATCHED THEN
// MAGIC INSERT (target.uid, target.convTimestamp, target.conversionID, target.advertiserID, target.pixelID, target.conversionValue) VALUES (new.uid, current_timestamp(), new.conversionID, new.advertiserID, new.pixelID, new.conversionValue)
// MAGIC 
// MAGIC ~~~~