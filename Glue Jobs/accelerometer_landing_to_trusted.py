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

Accel_Landing_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="accelerometer_landing",
    transformation_ctx="Accel_Landing_Node"
)

Customer_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="customer_trusted",
    transformation_ctx="Customer_Trusted_Node"
)

Join_Privacy_Node = Join.apply(
    frame1=Accel_Landing_Node,
    frame2=Customer_Trusted_Node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_Privacy_Node"
)

Privacy_Drop_Node = DropFields.apply(
    frame=Join_Privacy_Node,
    paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate"],
    transformation_ctx="Privacy_Drop_Node"
)

Accel_Trusted_Sink = glueContext.getSink(
    path="s3://stedi-1/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="Accel_Trusted_Sink"
)

Accel_Trusted_Sink.setFormat("json")

Accel_Trusted_Sink.setCatalogInfo(
    catalogDatabase="stedi-2",
    catalogTableName="accelerometer_trusted"
)

Accel_Trusted_Sink.writeFrame(Privacy_Drop_Node)

job.commit()