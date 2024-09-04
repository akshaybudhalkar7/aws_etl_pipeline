import boto3

def lambda_handler(event, context):
    client = boto3.client('glue')
    response = client.start_crawler(Name='{crawler.ref}')
    return response
