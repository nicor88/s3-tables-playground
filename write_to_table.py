import os
from dotenv import load_dotenv

load_dotenv(".env")

import boto3
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
import pyarrow as pa

client = boto3.client('s3tables', region_name='us-east-1')

S3_BUCKET_ARN = os.environ['S3_BUCKET_ARN']
namespace = 'playground'
table_name = 'example'


table_response = client.get_table(
    tableBucketARN=S3_BUCKET_ARN,
    namespace=namespace,
    name=table_name
)

warehouse_location_from_table = table_response['warehouseLocation']
print(warehouse_location_from_table)

current_folder = os.getcwd()

warehouse_local_location = os.path.join(current_folder, 'iceberg_warehouse')
full_path_warehouse_local_location= f'file://{warehouse_local_location}'

if not os.path.exists(warehouse_local_location):
    os.makedirs(warehouse_local_location)

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_local_location}/catalog.db",
        "warehouse": warehouse_location_from_table,
    },
)

df = pa.Table.from_pylist(
    [
        {"id": 1, "name": "user_1"},
        {"id": 2, "name": "user_2"},
    ],
)

try:
    catalog.create_namespace(namespace)
except NamespaceAlreadyExistsError as err:
    print(f'Namespace "{namespace}" already exists')


table = catalog.create_table(
    f"{namespace}.{table_name}",
    schema=df.schema,
)

# the above throw an error:
# AWS Error UNKNOWN (HTTP status 405) during ListObjectsV2 operation: Unable to parse ExceptionName: MethodNotAllowed Message: The specified method is not allowed against this resource.

table.append(df)
