import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import caseTransform
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node datasource1
datasource1_node1706541624082 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://<bucket>/<key>"
        ],
        "recurse": True,
    },
    transformation_ctx="datasource1_node1706541624082",
)

# Script generated for node datasource2
datasource2_node1706541624505 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://<bucket>/<key>"
        ],
        "recurse": True,
    },
    transformation_ctx="datasource2_node1706541624505",
)

# Script generated for node datasource1 - Change Schema
datasource1ChangeSchema_node1706543965207 = ApplyMapping.apply(
    frame=datasource1_node1706541624082,
    mappings=[("email address", "string", "email_address", "string")],
    transformation_ctx="datasource1ChangeSchema_node1706543965207",
)

# Script generated for node datasource2 - Change Schema
datasource2ChangeSchema_node1706543532073 = ApplyMapping.apply(
    frame=datasource2_node1706541624505,
    mappings=[("email_address", "string", "email_address", "string")],
    transformation_ctx="datasource2ChangeSchema_node1706543532073",
)

# Script generated for node datasource1 - Transform Case
datasource1TransformCase_node1706550495077 = (
    datasource1ChangeSchema_node1706543965207.case_transform(
        column_name="email_address", case="lowercase"
    )
)

# Script generated for node datasource2 - Transform Case
datasource2TransformCase_node1706550528183 = datasource2ChangeSchema_node1706543532073.case_transform(
    column_name="email_address", case="lowercase"
)

# Script generated for node datasource1 - Drop Duplicates
datasource1DropDuplicates_node1706544739218 = DynamicFrame.fromDF(
    datasource1TransformCase_node1706550495077.toDF().dropDuplicates(),
    glueContext,
    "datasource1DropDuplicates_node1706544739218",
)

# Script generated for node datasource2 - Drop Duplicates
datasource2DropDuplicates_node1706544759383 = DynamicFrame.fromDF(
    datasource2TransformCase_node1706550528183.toDF().dropDuplicates(),
    glueContext,
    "datasource2DropDuplicates_node1706544759383",
)

# Script generated for node Join
datasource1DropDuplicates_node1706544739218DF = (
    datasource1DropDuplicates_node1706544739218.toDF()
)
datasource2DropDuplicates_node1706544759383DF = datasource2DropDuplicates_node1706544759383.toDF()
Join_node1706544804116 = DynamicFrame.fromDF(
    datasource1DropDuplicates_node1706544739218DF.join(
        datasource2DropDuplicates_node1706544759383DF,
        (
            datasource1DropDuplicates_node1706544739218DF["email_address"]
            == datasource2DropDuplicates_node1706544759383DF["email_address"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1706544804116",
)

# Script generated for node Amazon S3
AmazonS3_node1706545029646 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1706544804116,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://<bucket>",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706545029646",
)

job.commit()
