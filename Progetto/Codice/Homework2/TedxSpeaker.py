###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, struct
import pyspark.sql.functions as func

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


##### FROM FILES---------------------------------------------------------
speaker_path = "s3://tedx-data-mz/speaker.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##--------------------------------------------------------------------------------------------


##----------------------------------------------------------------------------------------
#### READ INPUT FILES TO CREATE AN INPUT DATASET-----------------------------
speaker_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(speaker_path)
    
speaker_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = speaker_dataset.count()
count_items_null = speaker_dataset.filter("id is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")


#########################IL MAIN è SPEAKER POI AGGIUNGERE COME SOTTO CAMPI ID E ALTRE INFO DETTAGLIATE COME LINK PAR VIDEO....
## READ THE SPEAKER-----------------------------------------------------------------------

speaker_dataset = speaker_dataset.dropDuplicates(['id'])
#speaker_dataset.printSchema()                                       

tedx_dataset_main = speaker_dataset.groupBy('presenterDisplayName').count().select(func.col("count").alias("nrVideos"), func.col("presenterDisplayName"))
#tedx_dataset_main = speaker_dataset.select(col("id").alias("id_ref"),
 #                                        col("presenterDisplayName").alias("speaker"),
  #                                       col("duration"))



## READ FINAL LIST
#tedx_path = "s3://tedx-data-mz/final_list.csv"
#tedx_dataset = spark.read.option("header","true").csv(tedx_path)


# CREATE THE AGGREGATE MODEL
#tedx_dataset_agg = tedx_dataset.groupBy(col("speakers")).agg(collect_list(struct(col("id"), col("speakers"), col("title"), col("url"))))
#tedx_dataset_agg.printSchema()

##=====================================================================================================================

## LETTURA FINAL_LIST.CSV (ID | SPEAKERS | TITLE | URL)
tedx_path = "s3://tedx-data-mz/final_list.csv"
tedx_dataset = spark.read.option("header","true").csv(tedx_path)

tedx_dataset = tedx_dataset.select(col("id"),
                                         col("speakers"),
                                         col("title"),
                                         col("url"))

##  LETTURA DA DETAILS.CSV (ID | DURATION | publishedAt)
detail_path = "s3://tedx-data-mz/details.csv"
detail_dataset = spark.read.option("header","true").csv(detail_path)

detail_dataset = detail_dataset.select(col("id").alias("id_ref"),
                                         col("duration"),
                                         col("publishedAt"))

##  UNIONE COLONNE FINAL_LIST.CSV E DETAILS.CSV                          
tedx_dataset = tedx_dataset.join(detail_dataset, tedx_dataset.id == detail_dataset.id_ref, "left") \
    .drop("id_ref")                                      
tedx_dataset.printSchema()

                                         



# CREATE THE AGGREGATE MODEL
tedx_dataset_agg = tedx_dataset.groupBy(col("speakers")).agg(collect_list(struct(col("id"), col("speakers"), col("title"), col("url"), col("duration"), col("publishedAt"))))

#tedx_dataset_agg = tedx_dataset.groupBy(col("speakers")).agg(collect_list(struct(col("id"), col("speakers"), col("title"), col("url"))))
tedx_dataset_agg.printSchema()

##=====================================================================================================================
tedx_dataset_main = tedx_dataset_main.join(tedx_dataset_agg, tedx_dataset_main.presenterDisplayName == tedx_dataset_agg.speakers, "left") \
    .drop("speakers") \
    .select(col("presenterDisplayName").alias("_id"), col("*")) \
    .drop("presenterDisplayName") \
    .drop("id") \


tedx_dataset_main.printSchema()



## WRITE IN MONGODB
write_mongo_options = {
    "connectionName": "TEDX-MZ-2024",
    "database": "unibg_tedx_2024",
    "collection": "TedxSpeaker",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_main, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
