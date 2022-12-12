import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1670725414304 = glueContext.create_dynamic_frame.from_catalog(
    database="json_to_parquet",
    table_name="cleaned_parquet_data",
    transformation_ctx="AWSGlueDataCatalog_node1670725414304",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1670725410787 = glueContext.create_dynamic_frame.from_catalog(
    database="csv_to_parquet",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1670725410787",
)

# Script generated for node Join
Join_node1670725490100 = Join.apply(
    frame1=AWSGlueDataCatalog_node1670725410787,
    frame2=AWSGlueDataCatalog_node1670725414304,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1670725490100",
)

# Script generated for node Amazon S3
AmazonS3_node1670726199632 = glueContext.getSink(
    path="s3://final-project-analytics-bucket",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1670726199632",
)
AmazonS3_node1670726199632.setCatalogInfo(
    catalogDatabase="analytics_data", catalogTableName="report_data"
)
AmazonS3_node1670726199632.setFormat("glueparquet")
AmazonS3_node1670726199632.writeFrame(Join_node1670725490100)
job.commit()
