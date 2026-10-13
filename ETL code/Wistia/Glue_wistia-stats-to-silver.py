import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
from awsgluedq.transforms import EvaluateDataQuality

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
AmazonS3_node1791396802440 = glueContext.create_dynamic_frame.from_options(format_options={"jsonPath": "$[*]", "multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://insight-flow-project/bronze/wistia/stats/"]}, transformation_ctx="AmazonS3_node1791396802440")

# Script generated for node Flatten
Flatten_node1791397281271 = AmazonS3_node1791396802440.gs_flatten(maxLevels=1, separator="_")

# Script generated for node Change Schema
ChangeSchema_node1791611935903 = ApplyMapping.apply(frame=Flatten_node1791397281271, mappings=[("media_id", "string", "media_id", "string"), ("snapshot_timestamp", "string", "snapshot_timestamp", "timestamp"), ("stats_load_count", "int", "stats_load_count", "int"), ("stats_play_count", "int", "stats_play_count", "int"), ("stats_play_rate", "double", "stats_play_rate", "double"), ("stats_hours_watched", "double", "stats_hours_watched", "double"), ("stats_engagement", "double", "stats_engagement", "double"), ("stats_visitors", "int", "stats_visitors", "int")], transformation_ctx="ChangeSchema_node1791611935903")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1791611935903, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791392355204", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791397389560 = glueContext.getSink(path="s3://insight-flow-project/silver/wistia/stats/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791397389560")
AmazonS3_node1791397389560.setCatalogInfo(catalogDatabase="insight_flow_silver_db",catalogTableName="wistia_stats_transformed")
AmazonS3_node1791397389560.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791397389560.writeFrame(ChangeSchema_node1791611935903)
job.commit()