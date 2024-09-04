import boto3
import os

def handler(event, context):
    crawler_name = os.environ.get('GLUE_CRAWLER_NAME')
    client = boto3.client('glue')
    response = client.start_crawler(Name=crawler_name)
    return response


