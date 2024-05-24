###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



##### FROM FILES
tedx_dataset_path = "s3://tedx-data-mz/final_list.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("id is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ THE DETAILS
details_dataset_path = "s3://tedx-data-mz/details.csv"
details_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(details_dataset_path)

details_dataset = details_dataset.select(col("id").alias("id_ref"),
                                         col("description"),
                                         col("duration"),
                                         col("publishedAt"))

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------

## READ THE DETAILS IMAGES
details_dataset_path = "s3://tedx-data-mz/images.csv"
details_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(details_dataset_path)

details_dataset = details_dataset.select(col("id").alias("id_ref"),
                                         col("url").alias("urlImg"))
                                         

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------------

## ------------------------------------------------------------------------------------------------------------------
## WATCH NEXT
watch_next_path = "s3://tedx-data-mz/related_videos.csv"
watch_next = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(watch_next_path)

##watch_next_dataset = watch_next_dataset.select(col("id").alias("id_ref"),
##                                         col("related_id"))


##print(f"Number Watch_Next items (RAW): {watch_next_dataset.count()}")

watch_next = watch_next.dropDuplicates()

# ADD WATCH_NEXT TO AGGREGATE MODEL
watch_next_agg = watch_next.groupBy(col("id").alias("id_ref")).agg(collect_list("related_id").alias("watch_next"), collect_list("presenterDisplayName".alias("Speaker")))

watch_next_agg_related = watch_next.groupBy(col("related_id").alias("id_watch_next")).agg(collect_set("related_id").alias("id_watch_next"), collect_list("presenterDisplayName".alias("Speaker")))

## remove all related_id that not exist...
watch_next_agg = watch_next_agg.join(watch_next_agg_related, watch_next_agg.id_ref == watch_next_agg_related.id_watch_next, "left") \
    .drop("id_ref")
##<---------------------------------------

watch_next_dataset_agg.printSchema()

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(watch_next_agg, tedx_dataset.id == watch_next_agg.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------------

## READ TAGS DATASET
tags_dataset_path = "s3://tedx-data-mz/tags.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)


# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("id").alias("id_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_main.join(tags_dataset_agg, tedx_dataset.id == tags_dataset_agg.id_ref, "left") \
    .drop("id_ref") \
    .select(col("id").alias("_id"), col("*")) \
    .drop("id") \

tedx_dataset_agg.printSchema()


write_mongo_options = {
    "connectionName": "TEDX-MZ-2024",
    "database": "unibg_tedx_2024",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)



## ------------------------------------------------------------------------------------
## ========================================================================================================
## -----------------------------------------------------------------------------------
## ???????????????????????????????????????????????????????????????????????????????????????????????????????
## -----------------------------------------------------------------------------------



###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



##### FROM FILES
tedx_dataset_path = "s3://tedx-data-mz/final_list.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("id is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ THE DETAILS
details_dataset_path = "s3://tedx-data-mz/details.csv"
details_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(details_dataset_path)

details_dataset = details_dataset.select(col("id").alias("id_ref"),
                                         col("description"),
                                         col("duration"),
                                         col("publishedAt"))

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------

## READ THE DETAILS IMAGES
details_dataset_path = "s3://tedx-data-mz/images.csv"
details_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(details_dataset_path)

details_dataset = details_dataset.select(col("id").alias("id_ref"),
                                         col("url").alias("urlImg"))
                                         

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(details_dataset, tedx_dataset.id == details_dataset.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------------

## ------------------------------------------------------------------------------------------------------------------
## WATCH NEXT
watch_next_path = "s3://tedx-data-mz/related_videos.csv"
watch_next = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(watch_next_path)

##watch_next_dataset = watch_next_dataset.select(col("id").alias("id_ref"),
##                                         col("related_id"))


##print(f"Number Watch_Next items (RAW): {watch_next_dataset.count()}")

watch_next = watch_next.dropDuplicates()

# ADD WATCH_NEXT TO AGGREGATE MODEL
watch_next_agg = watch_next.groupBy(col("id").alias("id_ref")).agg(collect_list("related_id").alias("watch_next"))

watch_next_agg_related = watch_next.groupBy(col("related_id").alias("id_watch_next")).agg(collect_list("related_id").alias("id_watch_next"))

## remove all related_id that not exist...
watch_next_agg = watch_next_agg.join(watch_next_agg_related, watch_next_agg.id_ref == watch_next_agg_related.id_watch_next, "left")
##<---------------------------------------

watch_next_dataset_agg.printSchema()

# AND JOIN WITH THE MAIN TABLE
tedx_dataset_main = tedx_dataset.join(watch_next_agg, tedx_dataset.id == watch_next_agg.id_ref, "left") \
    .drop("id_ref")

tedx_dataset_main.printSchema()

## ------------------------------------------------------------------------------------------------------------------

## READ TAGS DATASET
tags_dataset_path = "s3://tedx-data-mz/tags.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)


# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("id").alias("id_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_main.join(tags_dataset_agg, tedx_dataset.id == tags_dataset_agg.id_ref, "left") \
    .drop("id_ref") \
    .select(col("id").alias("_id"), col("*")) \
    .drop("id") \

tedx_dataset_agg.printSchema()


write_mongo_options = {
    "connectionName": "TEDX-MZ-2024",
    "database": "unibg_tedx_2024",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)