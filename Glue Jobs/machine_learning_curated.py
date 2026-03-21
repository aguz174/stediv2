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
    database="stedi-2",
    table_name="steptrainer_trusted"
)

Accel_Trusted_Node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-2",
    table_name="accelerometer_trusted"
)

Step_Trainer_Trusted_Node.toDF().createOrReplaceTempView("steptrainer_trusted")
Accel_Trusted_Node.toDF().createOrReplaceTempView("accelerometer_trusted")

ML_Join_SQL = """
              SELECT
                  st.*,
                  at.x,
                  at.y,
                  at.z
              FROM steptrainer_trusted st
                       INNER JOIN accelerometer_trusted at
              ON st.sensorReadingTime = at.timestamp \
              """

SQL_DF = spark.sql(ML_Join_SQL)
ML_Curated_Transform = DynamicFrame.fromDF(SQL_DF, glueContext, "ML_Curated_Transform")

ML_Curated_Sink = glueContext.getSink(
    path="s3://stedi-1/machinelearning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True
)

ML_Curated_Sink.setFormat("json")

ML_Curated_Sink.setCatalogInfo(
    catalogDatabase="stedi-2",
    catalogTableName="machinelearning_curated"
)

ML_Curated_Sink.writeFrame(ML_Curated_Transform)
job.commit()
