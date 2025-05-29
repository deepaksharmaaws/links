import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
catalog_name = "glue_catalog"
warehouse_path = "s3://data-lake-raw-events-de-sandbox"
aws_region = "us-east-1"
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


def main():
    sql = f"select * from sampledb.elb_logs limit 10"
    spark_df = spark.sql(sql)
    spark_df.show()

if __name__ == "__main__":
    main()
