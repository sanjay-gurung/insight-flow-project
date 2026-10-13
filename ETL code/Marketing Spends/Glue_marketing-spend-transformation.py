import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1791403243552 = glueContext.create_dynamic_frame.from_options(format_options={"jsonPath": "$[*]", "multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://insight-flow-project/bronze/marketing_spend/"], "recurse": True}, transformation_ctx="AmazonS3_node1791403243552")

# Script generated for node Drop Duplicates
DropDuplicates_node1791404156550 =  DynamicFrame.fromDF(AmazonS3_node1791403243552.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1791404156550")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1791404156550, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791403667171", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791404218217 = glueContext.getSink(path="s3://insight-flow-project/silver/marekting_spend/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791404218217")
AmazonS3_node1791404218217.setCatalogInfo(catalogDatabase="insight_flow_silver_db",catalogTableName="marketing_spend_transformed")
AmazonS3_node1791404218217.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791404218217.writeFrame(DropDuplicates_node1791404156550)
job.commit()