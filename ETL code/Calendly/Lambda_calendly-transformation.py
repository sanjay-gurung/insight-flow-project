import json
import boto3
import urllib.parse
from datetime import datetime, timezone

s3 = boto3.client("s3")
glue = boto3.client("glue")

SILVER_BUCKET = "insight-flow-project"
SILVER_PREFIX = "silver/calendly/"

# IMPORTANT:
# Change this only if your existing Silver Glue database has a different name.
GLUE_DATABASE = "insight_flow_silver_db"
GLUE_TABLE = "calendly_transformed"


def ensure_glue_table():

    columns = [
        {"Name": "booking_id", "Type": "string"},
        {"Name": "invitee_uri", "Type": "string"},
        {"Name": "scheduled_event_uri", "Type": "string"},
        {"Name": "event_type_uri", "Type": "string"},

        {"Name": "webhook_event", "Type": "string"},
        {"Name": "webhook_created_at", "Type": "string"},

        {"Name": "invitee_name", "Type": "string"},
        {"Name": "invitee_email", "Type": "string"},
        {"Name": "invitee_status", "Type": "string"},
        {"Name": "invitee_timezone", "Type": "string"},
        {"Name": "invitee_created_at", "Type": "string"},

        {"Name": "event_name", "Type": "string"},
        {"Name": "event_status", "Type": "string"},
        {"Name": "event_start_time", "Type": "string"},
        {"Name": "event_end_time", "Type": "string"},
        {"Name": "event_created_at", "Type": "string"},

        {"Name": "employee_name", "Type": "string"},
        {"Name": "employee_email", "Type": "string"},

        {"Name": "utm_source", "Type": "string"},
        {"Name": "utm_medium", "Type": "string"},
        {"Name": "utm_campaign", "Type": "string"},
        {"Name": "utm_content", "Type": "string"},
        {"Name": "utm_term", "Type": "string"},

        {"Name": "processed_at", "Type": "string"}
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

            "Location": f"s3://{SILVER_BUCKET}/{SILVER_PREFIX}",

            "InputFormat":
                "org.apache.hadoop.mapred.TextInputFormat",

            "OutputFormat":
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",

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

        # Table already exists: keep definition synchronized
        glue.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )

        print(f"Glue table updated: {GLUE_DATABASE}.{GLUE_TABLE}")

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )

        print(f"Glue table created: {GLUE_DATABASE}.{GLUE_TABLE}")


def lambda_handler(event, context):

    # Get Bronze file from S3 ObjectCreated event
    record = event["Records"][0]["s3"]

    bucket = record["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["object"]["key"])

    print(f"Processing: s3://{bucket}/{key}")

    # Read Bronze JSON
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    raw_event = json.loads(
        response["Body"].read().decode("utf-8")
    )

    payload = raw_event.get("payload", {})
    scheduled_event = payload.get("scheduled_event", {})
    tracking = payload.get("tracking", {})

    # Employee / host information
    memberships = scheduled_event.get("event_memberships", [])

    employee_name = None
    employee_email = None

    if memberships:
        employee_name = memberships[0].get("user_name")
        employee_email = memberships[0].get("user_email")

    # Stable identifiers
    invitee_uri = payload.get("uri")
    scheduled_event_uri = scheduled_event.get("uri")
    event_type_uri = scheduled_event.get("event_type")

    booking_id = None

    if invitee_uri:
        booking_id = invitee_uri.rstrip("/").split("/")[-1]

    # Flatten Calendly record
    transformed = {

        # Booking identifiers
        "booking_id": booking_id,
        "invitee_uri": invitee_uri,
        "scheduled_event_uri": scheduled_event_uri,
        "event_type_uri": event_type_uri,

        # Webhook
        "webhook_event": raw_event.get("event"),
        "webhook_created_at": raw_event.get("created_at"),

        # Invitee
        "invitee_name": payload.get("name"),
        "invitee_email": payload.get("email"),
        "invitee_status": payload.get("status"),
        "invitee_timezone": payload.get("timezone"),
        "invitee_created_at": payload.get("created_at"),

        # Meeting / booking
        "event_name": scheduled_event.get("name"),
        "event_status": scheduled_event.get("status"),
        "event_start_time": scheduled_event.get("start_time"),
        "event_end_time": scheduled_event.get("end_time"),
        "event_created_at": scheduled_event.get("created_at"),

        # Employee / host
        "employee_name": employee_name,
        "employee_email": employee_email,

        # Marketing attribution
        "utm_source": tracking.get("utm_source"),
        "utm_medium": tracking.get("utm_medium"),
        "utm_campaign": tracking.get("utm_campaign"),
        "utm_content": tracking.get("utm_content"),
        "utm_term": tracking.get("utm_term"),

        # Pipeline metadata
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    # Keep same source filename in Silver
    filename = key.split("/")[-1]

    silver_key = f"{SILVER_PREFIX}{filename}"

    # Write flattened record to Silver
    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=silver_key,
        Body=json.dumps(transformed),
        ContentType="application/json"
    )

    print(
        f"Saved transformed file: "
        f"s3://{SILVER_BUCKET}/{silver_key}"
    )

    # Ensure Silver data is registered in Glue Data Catalog
    ensure_glue_table()

    return {
        "statusCode": 200,
        "booking_id": booking_id,
        "source_key": key,
        "silver_key": silver_key,
        "glue_database": GLUE_DATABASE,
        "glue_table": GLUE_TABLE
    }