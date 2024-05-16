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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1715879669530 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1715879669530")

# Script generated for node Customer Curated
CustomerCurated_node1715879751303 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1715879751303")

# Script generated for node SQL Query
SqlQuery0 = '''
select * 
from steptrainerlanding s
inner join customercurated c
on s.serialnumber = c.serialnumber
'''
SQLQuery_node1715879796276 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"steptrainerlanding":StepTrainerLanding_node1715879669530, "customercurated":CustomerCurated_node1715879751303}, transformation_ctx = "SQLQuery_node1715879796276")

# Script generated for node Drop Fields
DropFields_node1715880226942 = DropFields.apply(frame=SQLQuery_node1715879796276, paths=["customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1715880226942")

# Script generated for node Drop Duplicates
DropDuplicates_node1715880262457 =  DynamicFrame.fromDF(DropFields_node1715880226942.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715880262457")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1715880347265 = glueContext.getSink(path="s3://beta-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1715880347265")
StepTrainerTrusted_node1715880347265.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1715880347265.setFormat("json")
StepTrainerTrusted_node1715880347265.writeFrame(DropDuplicates_node1715880262457)
job.commit()