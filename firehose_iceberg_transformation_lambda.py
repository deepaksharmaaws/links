import sys
from awsglue.utils import getResolvedOptions
import time
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max
from boto3.dynamodb.conditions import Key
from dataclasses import dataclass,asdict,field
from typing import Optional

from datetime import datetime
@dataclass
class ProcessDetails:
    target_db:str
    target_table:str
    source_db: str
    source_table: str
    column_transformations: dict[str,str]
    item_count:int
    ordered_column_list:list[str]

@dataclass
class MappingDetail:
    column_name:str
    source_mapping:str
    field_order: int

@dataclass
class JobRunItem:
    layer_db_table_pk: str
    max_created_at:Optional[str] = field(default=None)
    job_status:Optional[str] = field(default=None)
    checked_at:Optional[str] = field(default=None)
    started_at:Optional[str] = field(default=None)
    ended_at:Optional[str] = field(default=None)
    source_row_count:Optional[str] = field(default="0"),
    target_row_count:Optional[str] = field(default="0")
    target_column_count:Optional[str] = field(default="0"),
    target_table:Optional[str] = field(default=None),
    status_message:Optional[str] = field(default="JOB_TRACKER")
    def __dict__(self):
        return {
            "layer_db_table_pk": self.layer_db_table_pk,
            "max_created_at": self.max_created_at,
            "job_status": self.job_status,
            "checked_at": self.checked_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "target_column_count": self.target_column_count,
            "status_message": self.status_message
        }
    
# Initialize job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'warehouse_path',
    'aws_region',
    'aws_account_id',
    'catalog_name'
])

catalog_name = args['catalog_name']
aws_region = args['aws_region']
aws_account_id = args['aws_account_id']
warehouse_path = args['warehouse_path']
pk_target_database = "demo_silver_db"
JOB_RUNNING:str = "RUNNING"
JOB_NOT_RUNNING:str = "NOT_RUNNING"
JOB_STARTING:str = "JOB_STARTING"
JOB_STATUS_TABLE:str = "test_event_b_s_job_run"
# pk_target_database = "silver_layer_database"
"""
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{catalog_name}.client.region", f"{aws_region}") \
    .config(f"spark.sql.catalog.{catalog_name}.glue.account-id", f"{aws_account_id}") \
    .config(f"spark.sql.iceberg.handle-timestamp-without-timezone","true") \
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled","true") \
    .config("spark.sql.catalog.glue_catalog.glue.id","925925731365") \
    .getOrCreate()
"""
spark = SparkSession.builder \
    .appName("IcebergWithSparkAndGlue") \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{catalog_name}.client.region", f"{aws_region}") \
    .config(f"spark.sql.iceberg.handle-timestamp-without-timezone","true") \
    .config("spark.sql.iceberg.check-ordering", "false") \
    .getOrCreate()
    
def query_table(table_name, partition_key_name, partition_key_value):
    """
    Query a DynamoDB table using its primary key.

    Args:
        table_name (str): The name of the DynamoDB table.
        partition_key_name (str): The name of the partition key attribute.
        partition_key_value (str): The value of the partition key to query.

    Returns:
        dict: The response from DynamoDB containing the query results.
    """
    # Initialize the DynamoDB resource
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Perform the query on the table's primary key
    response = table.query(KeyConditionExpression=Key(partition_key_name).eq(partition_key_value))

    return response
    
def query_gsi(table_name, index_name, partition_key_name, partition_key_value):
    """
    Query a Global Secondary Index (GSI) on a DynamoDB table.

    Args:
        table_name (str): The name of the DynamoDB table.
        index_name (str): The name of the Global Secondary Index.
        partition_key_name (str): The name of the GSI's partition key attribute.
        partition_key_value (str): The value of the GSI's partition key to query.

    Returns:
        dict: The response from DynamoDB containing the query results.
    """
    # Initialize the DynamoDB resource
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Perform the query on the GSI
    response = table.query(
        IndexName=index_name, KeyConditionExpression=Key(partition_key_name).eq(partition_key_value)
    )

    return response

def read_mappings(event_name:str)->tuple[ProcessDetails or None,int]:
    table_name:str = "test_iceberg_schema_registry"
    index_name:str = "layer_db_table_gsi-index"
    partition_key_name:str = "layer_db_table_gsi"
    partition_key_value:str = f"silver*{pk_target_database}*{event_name}"
    print(f"Getting mapping for {partition_key_name} value: {partition_key_value}")
    response = query_gsi(table_name,index_name,partition_key_name,partition_key_value)
    item_count: int = response["Count"] if response is not None and "Count" in response else 0

    if response is not None and "Count" in response and response["Count"] > 0 and "Items" in response:
        items = response["Items"]
        source_db = items[0]["source_db"]
        source_table = items[0]["source_table"]
        target_db = items[0]["db_name"]
        target_table = items[0]["table_name"]
        col_map_list:list[MappingDetail] = []
        for item in response["Items"]:
            col_map_list.append(MappingDetail(
                column_name=item["column_name"],
                source_mapping=item["source_mapping"],
                field_order=item["field_order"],
            ))
        sorted_by_col_map = sorted(col_map_list, key=lambda md: md.field_order)
        transformations: dict[str, col] = {}
        for md in sorted_by_col_map:
            transformations[md.column_name] = col(md.source_mapping)

        process_details: ProcessDetails = ProcessDetails(
            target_db =  target_db,
            target_table = target_table,
            source_db = source_db,
            source_table = source_table,
            column_transformations = transformations,
            item_count = item_count,
            ordered_column_list = [md.column_name for md in sorted_by_col_map],
        )
        return process_details,item_count
    else:
        return None, item_count

def check_running_job(event_name:str) -> JobRunItem:
    job_table_name:str = JOB_STATUS_TABLE
    partition_key_name:str = "layer_db_table_pk"
    partition_key_value:str = f"silver*{pk_target_database}*{event_name}"
    response = query_table(job_table_name,partition_key_name,partition_key_value)
    record_count: int = response["Count"]
    if record_count == 0:
        print(f"No running jobs found for {event_name}")
        return JobRunItem(
            layer_db_table_pk = partition_key_value,
            max_created_at = "",
            job_status = JOB_NOT_RUNNING,
            checked_at = datetime.now().isoformat(),
            started_at = "",
            ended_at = "",
            source_row_count = "",
            target_row_count = "",
            target_column_count = "",
            status_message = f"No running jobs found for {event_name}"
            )
    else:
        print(f"Items found for {event_name}")
        job_status:str = response['Items'][0]['job_status'] if 'job_status' in response['Items'][0] else JOB_NOT_RUNNING
        job_running = job_status ==JOB_RUNNING
        return JobRunItem(
            layer_db_table_pk = partition_key_value,
            max_created_at = response['Items'][0]['max_created_at'] if 'max_created_at' in response['Items'][0] else "",
            job_status = response['Items'][0]['job_status'] if 'job_status' in response['Items'][0] else JOB_NOT_RUNNING ,
            started_at = response['Items'][0]['started_at'] if 'started_at' in response['Items'][0] else "",
            ended_at = response['Items'][0]['ended_at'] if 'ended_at' in response['Items'][0] else "",
            checked_at = response['Items'][0]['checked_at'] if 'checked_at' in response['Items'][0] else "",
            source_row_count =  response['Items'][0]['source_row_count'] if 'source_row_count' in response['Items'][0] else "",
            target_row_count =  response['Items'][0]['target_row_count'] if 'target_row_count' in response['Items'][0] else "",
            target_column_count =  response['Items'][0]['target_column_count'] if 'target_column_count' in response['Items'][0] else "",
            status_message =  response['Items'][0]['status_message'] if 'status_message' in response['Items'][0] else "",
            )

def update_job_status(job_run_item: JobRunItem):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(JOB_STATUS_TABLE)
    table.put_item(
        Item= job_run_item.__dict__()
    )

    
def main(event_name:str):
    job_run:JobRunItem = check_running_job(event_name)
    if job_run.job_status is JOB_RUNNING or job_run.job_status is JOB_STARTING:
        job_run.checked_at = datetime.now().isoformat()
        update_job_status(job_run)
        print(f"Job is for event {event_name} is {job_run.job_status}. Cannot run 2 jobs at same time.")
    else:
        print(f"NO Job is running for event {event_name}. Do work")
        # mark job as JOB_STARTING
        job_run.job_status = JOB_STARTING
        job_run.started_at = datetime.now().isoformat()
        job_run.checked_at = datetime.now().isoformat()
        job_run.status_message = "Job starting"
        update_job_status(job_run)
        mapping_result: tuple [ProcessDetails or None,int] = read_mappings(event_name)
        pd:ProcessDetails = mapping_result[0]
        target_identifier:str = f"{catalog_name}.{pd.target_db}.{pd.target_table}"
        job_run.target_table = target_identifier
        item_count: int = mapping_result[1]
        print(f"Item count: {item_count}")
        if item_count > 0:
            job_run.job_status = JOB_RUNNING
            job_run.started_at = datetime.now().isoformat()
            job_run.status_message = "Job started"
            update_job_status(job_run)
            if job_run.max_created_at is not None and len(job_run.max_created_at) > 0:
                max_created_at = datetime.fromisoformat(job_run.max_created_at)
                print(f"max date found as {max_created_at}")
                source_sql = f"SELECT * FROM {catalog_name}.{pd.source_db}.{pd.source_table} where _created_at > '{max_created_at}' limit 10"
            else:
                source_sql = f"SELECT * FROM {catalog_name}.{pd.source_db}.{pd.source_table} limit 10"
            print(source_sql)
            s_df = spark.sql(source_sql)
            s_row_count:int = s_df.count()
            job_run.source_row_count = s_row_count
            print(f"S Row Count: {s_row_count}")
            if s_row_count > 0:
                # make field name parameter or configurable from DDB table?
                max_created_at = s_df.select(max("_created_at")).collect()[0][0]
                job_run.max_created_at = max_created_at.isoformat()
                print(f"{max_created_at} of type {type(max_created_at)}")
                print(f"transformation list: {len(pd.column_transformations)}. source_db_table = {pd.source_db}.{pd.source_table}")
                t_df = s_df.withColumns(pd.column_transformations).select(*pd.ordered_column_list)
                print(f"T column count: {len(t_df.columns)}")
                job_run.target_column_count = len(t_df.columns)
                job_run.target_row_count = t_df.count()
                
                print(f"Writing to TI: {target_identifier}")
                t_df.writeTo(target_identifier) \
                    .tableProperty("format-version","2") \
                    .append()
                job_run.ended_at = datetime.now().isoformat()
                job_run.job_status = JOB_NOT_RUNNING
                job_run.status_message = "Job not running"
            else:
                job_run.job_status = JOB_NOT_RUNNING
            update_job_status(job_run)
        else: # no items found in mapping mark job as done
            job_run.job_status = JOB_NOT_RUNNING
            job_run.ended_at = datetime.now().isoformat()
            update_job_status(job_run)
 

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               'event_name'])
    event_name:str = args['event_name']                    
    print(f"Event name is: {args['event_name']}")
    main(event_name)

 
