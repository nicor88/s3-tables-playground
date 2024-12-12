import os
from dotenv import load_dotenv

load_dotenv(".env")

import boto3

client = boto3.client('s3tables', region_name='us-east-1')

S3_BUCKET_NAME = os.environ['S3_TABLE_BUCKET']
S3_BUCKET_ARN = os.environ['S3_BUCKET_ARN']

namespace = 'playground'
table_name = 'example'



try:
    response = client.create_namespace(
        tableBucketARN=S3_BUCKET_ARN,
        namespace=[
            namespace,
        ]
    )
except Exception as err:
    code = err.response.get('Error').get('Code')
    if code == 'ConflictException':
        print(f'skipping, namespace {namespace} already exists')
    else:
        raise err

try:
    response_create_table = client.create_table(
        tableBucketARN=S3_BUCKET_ARN,
        namespace=namespace,
        name=table_name,
        format='ICEBERG'
    )
except Exception as err:
    code = err.response.get('Error').get('Code')
    if code == 'ConflictException':
        print(f'skipping, table {table_name} already exists in namespace {namespace}')
    else:
        raise err


table_metadata_response = client.get_table_metadata_location(
    tableBucketARN=S3_BUCKET_ARN,
    namespace=namespace,
    name=table_name
)
warehouse_location = table_metadata_response['warehouseLocation']
print(warehouse_location)

table_response = client.get_table(
    tableBucketARN=S3_BUCKET_ARN,
    namespace=namespace,
    name=table_name
)

warehouse_location_from_table = table_response['warehouseLocation']
print(warehouse_location_from_table)
