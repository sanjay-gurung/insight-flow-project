import json
import boto3
import urllib.parse
import urllib.request

s3 = boto3.client("s3")
sns = boto3.client("sns")
glue = boto3.client("glue")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SILVER_BUCKET = "insight-flow-project"
SILVER_PREFIX = "silver/crm_leads/"

# IMPORTANT:
# Use the SAME Silver database where calendly_transformed,
# marketing_spend_transformed, and Wistia tables exist.
GLUE_DATABASE = "insight_flow_silver_db"

GLUE_TABLE = "crm_leads_transformed"

SNS_TOPIC_ARN = (
    "arn:aws:sns:us-east-2:951411651540:"
    "insight-flow-lead-notifications"
)


# ---------------------------------------------------------
# Create/update Glue Data Catalog table
# ---------------------------------------------------------

def ensure_glue_table():

    columns = [
        {"Name": "lead_id", "Type": "string"},
        {"Name": "display_name", "Type": "string"},
        {"Name": "lead_email", "Type": "string"},
        {"Name": "lead_owner", "Type": "string"},
        {"Name": "funnel", "Type": "string"},
        {"Name": "status_label", "Type": "string"},
        {"Name": "date_created", "Type": "string"},
        {"Name": "crm_event_id", "Type": "string"},
        {"Name": "crm_action", "Type": "string"},
        {"Name": "crm_data", "Type": "string"}
    ]

    table_input = {
        "Name": GLUE_TABLE,
        "TableType": "EXTERNAL_TABLE",

        "Parameters": {
            "classification": "json",
            "EXTERNAL": "TRUE"
        },

        "StorageDescriptor": {

            "Columns": columns,

            "Location": (
                f"s3://{SILVER_BUCKET}/{SILVER_PREFIX}"
            ),

            "InputFormat":
                "org.apache.hadoop.mapred.TextInputFormat",

            "OutputFormat":
                "org.apache.hadoop.hive.ql.io."
                "HiveIgnoreKeyTextOutputFormat",

            "SerdeInfo": {
                "SerializationLibrary":
                    "org.openx.data.jsonserde.JsonSerDe",
                "Parameters": {}
            }
        }
    }

    try:

        glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=GLUE_TABLE
        )

        glue.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )

        print(
            f"Glue table updated: "
            f"{GLUE_DATABASE}.{GLUE_TABLE}"
        )

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )

        print(
            f"Glue table created: "
            f"{GLUE_DATABASE}.{GLUE_TABLE}"
        )


# ---------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------

def lambda_handler(event, context):

    processed_records = []

    for record in event["Records"]:

        # -------------------------------------------------
        # 1. Read S3 event from SQS message
        # -------------------------------------------------

        s3_event = json.loads(record["body"])

        bucket = (
            s3_event["Records"][0]["s3"]
            ["bucket"]["name"]
        )

        key = (
            s3_event["Records"][0]["s3"]
            ["object"]["key"]
        )

        key = urllib.parse.unquote_plus(key)

        print("CRM Bucket:", bucket)
        print("CRM S3 Key:", key)

        # -------------------------------------------------
        # 2. Read original CRM webhook file from Bronze
        # -------------------------------------------------

        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        crm_event = json.loads(
            response["Body"].read().decode("utf-8")
        )

        # -------------------------------------------------
        # 3. Extract Lead ID
        # -------------------------------------------------

        lead_id = crm_event["event"]["lead_id"]

        print("Lead ID:", lead_id)

        # -------------------------------------------------
        # 4. Retrieve Lead Owner enrichment data
        # -------------------------------------------------

        lead_owner_url = (
            "https://dea-lead-owner.s3.us-east-1."
            f"amazonaws.com/{lead_id}.json"
        )

        print("Lead Owner URL:", lead_owner_url)

        with urllib.request.urlopen(
            lead_owner_url
        ) as response:

            lead_owner_data = json.loads(
                response.read().decode("utf-8")
            )

        print("Lead Owner Data:")
        print(json.dumps(lead_owner_data))

        # -------------------------------------------------
        # 5. Get CRM data
        # -------------------------------------------------

        crm_data = crm_event["event"].get(
            "data",
            {}
        )

        # -------------------------------------------------
        # 6. Create flattened/enriched Silver record
        # -------------------------------------------------

        enriched_lead = {

            "lead_id":
                lead_id,

            "display_name":
                lead_owner_data.get("display_name"),

            "lead_email":
                lead_owner_data.get("lead_email"),

            "lead_owner":
                lead_owner_data.get("lead_owner"),

            "funnel":
                lead_owner_data.get("funnel"),

            "status_label":
                lead_owner_data.get("status_label"),

            "date_created":
                lead_owner_data.get("date_created"),

            "crm_event_id":
                crm_event["event"].get("id"),

            "crm_action":
                crm_event["event"].get("action"),

            # Store nested CRM payload as JSON text
            # so Athena schema stays simple and reliable.
            "crm_data":
                json.dumps(crm_data)
        }

        # -------------------------------------------------
        # 7. Write enriched lead to Silver
        #
        # lead_id gives us a stable output key.
        # Reprocessing the same lead overwrites the file
        # instead of creating duplicate Silver files.
        # -------------------------------------------------

        output_key = (
            f"{SILVER_PREFIX}{lead_id}.json"
        )

        s3.put_object(
            Bucket=SILVER_BUCKET,
            Key=output_key,
            Body=json.dumps(enriched_lead),
            ContentType="application/json"
        )

        print(
            "Enriched lead saved to:",
            output_key
        )

        # -------------------------------------------------
        # 8. Send SNS notification
        # -------------------------------------------------

        message = f"""
New Close CRM Lead

Name: {enriched_lead.get("display_name")}
Lead ID: {enriched_lead.get("lead_id")}
Created Date: {enriched_lead.get("date_created")}
Label: {enriched_lead.get("status_label")}
Email: {enriched_lead.get("lead_email")}
Lead Owner: {enriched_lead.get("lead_owner")}
Funnel: {enriched_lead.get("funnel")}
"""

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="New Close CRM Lead",
            Message=message
        )

        print(
            "Email notification published to SNS"
        )

        processed_records.append({
            "lead_id": lead_id,
            "silver_key": output_key
        })

    # -----------------------------------------------------
    # 9. Ensure Glue Catalog table exists
    # -----------------------------------------------------

    ensure_glue_table()

    # -----------------------------------------------------
    # 10. Return result
    # -----------------------------------------------------

    return {
        "statusCode": 200,
        "processed_records": processed_records,
        "glue_database": GLUE_DATABASE,
        "glue_table": GLUE_TABLE
    }