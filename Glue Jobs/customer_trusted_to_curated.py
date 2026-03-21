import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

Customer_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="customer_trusted"
)

Accel_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="accelerometer_trusted"
)

Customer_Trusted_Node.toDF().createOrReplaceTempView("customer_trusted")
Accel_Trusted_Node.toDF().createOrReplaceTempView("accelerometer_trusted")

SQL_DF = spark.sql("""
                   SELECT DISTINCT c.* FROM customer_trusted c
                                                JOIN accelerometer_trusted a ON c.email = a.user
                   """)

Customer_Curated_Sink = glueContext.getSink(
    path="s3://stedi-1/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)

Customer_Curated_Sink.setFormat("json")
Customer_Curated_Sink.setCatalogInfo(
    catalogDatabase="stedi-2",
    catalogTableName="customer_curated"
)

dynamic_frame_to_write = DynamicFrame.fromDF(SQL_DF, glueContext, "dynamic_frame_to_write")


Customer_Curated_Sink.writeFrame(dynamic_frame_to_write)
job.commit()