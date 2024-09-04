import boto3
import os
import json

def handler(event, context):
    crawler_name_list = os.environ.get('GLUE_CRAWLER_NAME')
    client = boto3.client('glue', region_name='us-east-1')
    crawler_name_list = json.loads(crawler_name_list)
    for crawler_name in crawler_name_list:
        response = client.start_crawler(Name=crawler_name)
        return response



