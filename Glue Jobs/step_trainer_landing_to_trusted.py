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

Step_Landing_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="step_trainer_landing",
    transformation_ctx="Step_Landing_Node"
)

Customer_Curated_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="customer_curated",
    transformation_ctx="Customer_Curated_Node"
)

Step_Join_Node = Join.apply(
    frame1=Step_Landing_Node,
    frame2=Customer_Curated_Node,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Step_Join_Node"
)

Step_Drop_Node = DropFields.apply(
    frame=Step_Join_Node,
    paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate"],
    transformation_ctx="Step_Drop_Node"
)

Step_Trusted_Sink = glueContext.getSink(
    path="s3://YOUR_BUCKET/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="Step_Trusted_Sink"
)

Step_Trusted_Sink.setCatalogInfo(
    catalogDatabase="YOUR_DATABASE",
    catalogTableName="step_trainer_trusted"
)

Step_Trusted_Sink.writeFrame(Step_Drop_Node)

job.commit()
