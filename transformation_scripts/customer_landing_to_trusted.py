import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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

# Script generated for node Customer Landing
CustomerLanding_node1715712304244 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://beta-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1715712304244")

# Script generated for node Share With Research
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate IS NOT NULL AND shareWithResearchAsOfDate <> 0
'''
ShareWithResearch_node1715840040105 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1715712304244}, transformation_ctx = "ShareWithResearch_node1715840040105")

# Script generated for node Customer Trusted
CustomerTrusted_node1715712873860 = glueContext.getSink(path="s3://beta-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1715712873860")
CustomerTrusted_node1715712873860.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1715712873860.setFormat("json")
CustomerTrusted_node1715712873860.writeFrame(ShareWithResearch_node1715840040105)
job.commit()