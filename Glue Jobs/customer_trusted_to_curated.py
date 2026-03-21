import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

Customer_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="customer_trusted",
    transformation_ctx="Customer_Trusted_Node"
)

Accel_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="accelerometer_trusted",
    transformation_ctx="Accel_Trusted_Node"
)

Curated_Join_Node = Join.apply(
    frame1=Customer_Trusted_Node,
    frame2=Accel_Trusted_Node,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Curated_Join_Node"
)

Drop_Duplicate_Node = DropFields.apply(
    frame=Curated_Join_Node,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="Drop_Duplicate_Node"
)

Customer_Curated_Sink = glueContext.getSink(
    path="s3://YOUR_BUCKET/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="Customer_Curated_Sink"
)

Customer_Curated_Sink.setCatalogInfo(
    catalogDatabase="YOUR_DATABASE",
    catalogTableName="customer_curated"
)

Customer_Curated_Sink.writeFrame(Drop_Duplicate_Node)

job.commit()
