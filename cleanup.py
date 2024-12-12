import os
from dotenv import load_dotenv

load_dotenv(".env")

S3_BUCKET_ARN = os.environ['S3_BUCKET_ARN']
namespace = 'playground'
table_name = 'test_table'


client.delete_table(
    tableBucketARN=S3_BUCKET_ARN,
    namespace=namespace,
    name=table_name
)

client.delete_namespace(
    tableBucketARN=S3_BUCKET_ARN,
    namespace=namespace
)

client.delete_table_bucket(
    tableBucketARN=S3_BUCKET_ARN
)
