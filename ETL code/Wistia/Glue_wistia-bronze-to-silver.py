import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
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
AmazonS3_node1791325592996 = glueContext.create_dynamic_frame.from_options(format_options={"jsonPath": "$[*]", "multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://insight-flow-project/bronze/wistia/events/"]}, transformation_ctx="AmazonS3_node1791325592996")

# Script generated for node Change Schema
ChangeSchema_node1791327003969 = ApplyMapping.apply(frame=AmazonS3_node1791325592996, mappings=[("received_at", "string", "received_at", "string"), ("event_key", "string", "event_key", "string"), ("ip", "string", "ip", "string"), ("country", "string", "country", "string"), ("region", "string", "region", "string"), ("city", "string", "city", "string"), ("email", "null", "email", "null"), ("percent_viewed", "double", "percent_viewed", "double"), ("embed_url", "string", "embed_url", "string"), ("visitor_key", "string", "visitor_key", "string"), ("user_agent_details.browser", "string", "user_agent_details.browser", "string"), ("user_agent_details.browser_version", "string", "user_agent_details.browser_version", "string"), ("user_agent_details.platform", "string", "user_agent_details.platform", "string"), ("user_agent_details.mobile", "boolean", "user_agent_details.mobile", "boolean"), ("media_id", "string", "media_id", "string"), ("media_name", "string", "media_name", "string"), ("media_url", "string", "media_url", "string")], transformation_ctx="ChangeSchema_node1791327003969")

# Script generated for node Flatten
Flatten_node1791347172189 = ChangeSchema_node1791327003969.gs_flatten(maxLevels=1, separator="_")

# Script generated for node Drop Duplicates
DropDuplicates_node1791345742878 =  DynamicFrame.fromDF(Flatten_node1791347172189.toDF().dropDuplicates(["event_key"]), glueContext, "DropDuplicates_node1791345742878")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1791345742878, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791325567085", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791345975725 = glueContext.getSink(path="s3://insight-flow-project/silver/wistia/events/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791345975725")
AmazonS3_node1791345975725.setCatalogInfo(catalogDatabase="insight_flow_silver_db",catalogTableName="wistia_events_transformed")
AmazonS3_node1791345975725.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791345975725.writeFrame(DropDuplicates_node1791345742878)
job.commit()