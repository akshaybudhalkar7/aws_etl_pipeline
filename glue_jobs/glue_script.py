# glue_jobs/my_glue_job.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


albums_data = glueContext.create_dynamic_frame.from_catalog(
    database ='etl_pipline',
    table_name ='albums_data'
)

artists_data = glueContext.create_dynamic_frame.from_catalog(
    database= 'etl_pipeline',
    table_name = 'artist_data'
)

joinalbumartist=Join.apply(frame1=albums_data,frame2=artists_data,keys1=['artist_id'],keys2=['id'],transformation_ctx="joinalbumartist")
f
job.commit()
