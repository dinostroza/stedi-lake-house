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

# Script generated for node Customer Trusted
CustomerTrusted_node1715875544656 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1715875544656")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1715875548757 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1715875548757")

# Script generated for node SQL Query
SqlQuery0 = '''
select * 
from customertrusted c
inner join
accelerometertrusted a
on
c.email = a.user
'''
SQLQuery_node1715875859673 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customertrusted":CustomerTrusted_node1715875544656, "accelerometertrusted":AccelerometerTrusted_node1715875548757}, transformation_ctx = "SQLQuery_node1715875859673")

# Script generated for node Drop Fields
DropFields_node1715876033402 = DropFields.apply(frame=SQLQuery_node1715875859673, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1715876033402")

# Script generated for node Drop Duplicates
DropDuplicates_node1715877772891 =  DynamicFrame.fromDF(DropFields_node1715876033402.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715877772891")

# Script generated for node Customer Curated
CustomerCurated_node1715876071663 = glueContext.getSink(path="s3://beta-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1715876071663")
CustomerCurated_node1715876071663.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1715876071663.setFormat("json")
CustomerCurated_node1715876071663.writeFrame(DropDuplicates_node1715877772891)
job.commit()