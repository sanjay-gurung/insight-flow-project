import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET_NAME = "insight-flow-project"


def lambda_handler(event, context):

    print("Calendly webhook received")

    # Get webhook body from API Gateway
    body = event.get("body", "{}")

    if isinstance(body, str):
        calendly_event = json.loads(body)
    else:
        calendly_event = body

    print("Webhook body:")
    print(json.dumps(calendly_event))

    # Create unique file name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

    key = f"bronze/calendly/calendly_event_{timestamp}.json"

    # Save raw Calendly webhook JSON to Bronze
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(calendly_event),
        ContentType="application/json"
    )

    print("Saved Calendly event to S3:", key)

    return {
        "statusCode": 200,
        "body": json.dumps("Calendly webhook received successfully")
    }