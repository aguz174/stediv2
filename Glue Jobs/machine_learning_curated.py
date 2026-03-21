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

Step_Trainer_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="step_trainer_trusted",
    transformation_ctx="Step_Trainer_Trusted_Node"
)

Accel_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="YOUR_DATABASE",
    table_name="accelerometer_trusted",
    transformation_ctx="Accel_Trusted_Node"
)

ML_Join_SQL = """
              SELECT
                  st.*,
                  at.x,
                  at.y,
                  at.z
              FROM
                  step_trainer_trusted st
                      INNER JOIN
                  accelerometer_trusted at
              ON
                  st.sensorReadingTime = at.timestamp
              """

ML_Curated_Transform = sparkSqlQuery(
    glueContext,
    query=ML_Join_SQL,
    mapping={
        "step_trainer_trusted": Step_Trainer_Trusted_Node,
        "accelerometer_trusted": Accel_Trusted_Node
    },
    transformation_ctx="ML_Curated_Transform"
)

ML_Curated_Sink = glueContext.getSink(
    path="s3://YOUR_BUCKET/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    transformation_ctx="ML_Curated_Sink"
)

ML_Curated_Sink.setCatalogInfo(
    catalogDatabase="YOUR_DATABASE",
    catalogTableName="machine_learning_curated"
)

ML_Curated_Sink.writeFrame(ML_Curated_Transform)

job.commit()
