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

Customer_Landing_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="customer_landing",
    transformation_ctx="Customer_Landing_Node"
)

Share_With_Research_Node = Filter.apply(
    frame=Customer_Landing_Node,
    f=lambda x: x["shareWithResearchAsOfDate"] is not None,
    transformation_ctx="Share_With_Research_Node"
)

Drop_Serial_Number_Node = DropFields.apply(
    frame=Share_With_Research_Node,
    paths=["serialNumber"],
    transformation_ctx="Drop_Serial_Number_Node"
)

Customer_Trusted_Sink = glueContext.getSink(
    path="s3://stedi-1/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="Customer_Trusted_Sink"
)

Customer_Trusted_Sink.setCatalogInfo(
    catalogDatabase="stedi-2",
    catalogTableName="customer_trusted"
)

Customer_Trusted_Sink.writeFrame(Drop_Serial_Number_Node)

job.commit()