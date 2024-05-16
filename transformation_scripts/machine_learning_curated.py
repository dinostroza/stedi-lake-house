import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1715881345881 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1715881345881")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1715881281623 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1715881281623")

# Script generated for node SQL Query
SqlQuery0 = '''
select * 
from steptrainertrusted s
inner join accelerometertrusted a
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1715881394407 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"steptrainertrusted":StepTrainerTrusted_node1715881345881, "accelerometertrusted":AccelerometerTrusted_node1715881281623}, transformation_ctx = "SQLQuery_node1715881394407")

# Script generated for node Drop Duplicates
DropDuplicates_node1715881907267 =  DynamicFrame.fromDF(SQLQuery_node1715881394407.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715881907267")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1715881916409 = glueContext.getSink(path="s3://beta-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1715881916409")
MachineLearningCurated_node1715881916409.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1715881916409.setFormat("json")
MachineLearningCurated_node1715881916409.writeFrame(DropDuplicates_node1715881907267)
job.commit()