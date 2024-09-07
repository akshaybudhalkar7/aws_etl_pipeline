import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['JOB_NAME','target-bucket'])
# Access the parameters
target_bucket = args['target-bucket']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_album = glueContext.create_dynamic_frame.from_catalog(database="etl_pipline", table_name="albums_data", transformation_ctx="AWSGlueDataCatalog_album")

AWSGlueDataCatalog_artist = glueContext.create_dynamic_frame.from_catalog(database="etl_pipline", table_name="artist_data", transformation_ctx="AWSGlueDataCatalog_artist")

album_artist_join = Join.apply(frame1=AWSGlueDataCatalog_album, frame2=AWSGlueDataCatalog_artist, keys1=["id"], keys2=["artist_id"], transformation_ctx="album_artist_join")

tracks_data = glueContext.create_dynamic_frame.from_catalog(database="etl_pipline", table_name="tracks_data", transformation_ctx="tracks_data")

joinwithtracks = Join.apply(frame1=album_artist_join, frame2=tracks_data,keys1=['track_id'],keys2=['track_id'],transformation_ctx="joinwithtracks")

dropfields = dropfields.apply(frame=joinwithtracks,paths=["`.track_id`","id"],transformation_ctx="dropfields")


path = f's3://{target_bucket}/datawarehouse/'

# Script generated for node Amazon S3
destination_s3= glueContext.write_dynamic_frame.from_options(frame=dropfields, connection_type="s3", format="glueparquet", connection_options={"path": path, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="destination_s3")



job.commit()

