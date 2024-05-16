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
CustomerTrusted_node1715720327811 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1715720327811")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1715876592048 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1715876592048")

# Script generated for node Share With Research
SqlQuery0 = '''
select * 
from accelerometerlanding a 
inner join 
customertrusted c 
on a.user = c.email

'''
ShareWithResearch_node1715870656349 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customertrusted":CustomerTrusted_node1715720327811, "accelerometerlanding":AccelerometerLanding_node1715876592048}, transformation_ctx = "ShareWithResearch_node1715870656349")

# Script generated for node Drop Customer Fields
DropCustomerFields_node1715721154736 = DropFields.apply(frame=ShareWithResearch_node1715870656349, paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropCustomerFields_node1715721154736")

# Script generated for node Drop Duplicates
DropDuplicates_node1715877999930 =  DynamicFrame.fromDF(DropCustomerFields_node1715721154736.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715877999930")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1715720968374 = glueContext.getSink(path="s3://beta-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1715720968374")
AccelerometerTrusted_node1715720968374.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1715720968374.setFormat("json")
AccelerometerTrusted_node1715720968374.writeFrame(DropDuplicates_node1715877999930)
job.commit()