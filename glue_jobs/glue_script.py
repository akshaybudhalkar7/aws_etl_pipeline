import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_and_print(message):
    logger.info(message)
    print(message)

# Get script arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_bucket'])
log_and_print(f"Arguments: {args}")

# Access the parameters
target_bucket = args['target_bucket']
log_and_print(f"Target bucket: {target_bucket}")

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

log_and_print("Spark and Glue contexts initialized")

try:
    # Load data from AWS Glue Data Catalog
    log_and_print("Loading data from AWS Glue Data Catalog")
    # AWSGlueDataCatalog_album = glueContext.create_dynamic_frame.from_catalog(
    #     database="etl_pipline",
    #     table_name="albums_data",
    #     transformation_ctx="AWSGlueDataCatalog_album"
    # )

    AWSGlueDataCatalog_artist = glueContext.create_dynamic_frame.from_catalog(
        database="etl_pipline",
        table_name="artist_data",
        transformation_ctx="AWSGlueDataCatalog_artist"
    )

    log_and_print("Data loaded successfully from Data Catalog")

    # # Perform join operations
    # log_and_print("Performing join operations")
    # album_artist_join = Join.apply(
    #     frame1=AWSGlueDataCatalog_album,
    #     frame2=AWSGlueDataCatalog_artist,
    #     keys1=["id"],
    #     keys2=["artist_id"],
    #     transformation_ctx="album_artist_join"
    # )

    # tracks_data = glueContext.create_dynamic_frame.from_catalog(
    #     database="etl_pipline",
    #     table_name="tracks_data",
    #     transformation_ctx="tracks_data"
    # )

    # joinwithtracks = Join.apply(
    #     frame1=album_artist_join,
    #     frame2=tracks_data,
    #     keys1=['track_id'],
    #     keys2=['track_id'],
    #     transformation_ctx="joinwithtracks"
    # )

    log_and_print("Join operations completed")

    # # Drop unnecessary fields
    # log_and_print("Dropping unnecessary fields")
    # dropfields = DropFields.apply(
    #     frame=joinwithtracks,
    #     paths=["id"],
    #     transformation_ctx="dropfields"
    # )

    # log_and_print("Fields dropped")

    # Define S3 path and write data to S3
    path = f's3://{target_bucket}/datawarehouse/'
    log_and_print(f"Writing data to S3 path: {path}")

    destination_s3 = glueContext.write_dynamic_frame.from_options(
        frame=AWSGlueDataCatalog_artist,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": "s3://snowman-etl-pipline-spotifydatabucket4e50a02c-4rpkfsyzyaak/datawarehouse/", "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="destination_s3"
    )

    log_and_print("Data written to S3 successfully")

except Exception as e:
    log_and_print(f"Error: {str(e)}")
    raise

finally:
    job.commit()
    log_and_print("Job committed")
