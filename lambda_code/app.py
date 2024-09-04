import boto3
import os

def handler(event, context):
    crawler_name_list  = os.environ.get('GLUE_CRAWLER_NAME')
    client = boto3.client('glue')
    for crawler_name in crawler_name_list:
        response = client.start_crawler(Name=crawler_name)
        return response



