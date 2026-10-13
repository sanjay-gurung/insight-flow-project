import json
import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):

    print("CRM webhook received")

    body = event.get("body")

    if body:
        body = json.loads(body)

    print("Webhook body:", body)

    # Get lead_id from the CRM webhook event
    lead_id = body["event"]["lead_id"]

    # Create the required S3 filename
    file_path = f"bronze/close_crm/crm_event_{lead_id}.json"

    # Save CRM event to S3
    s3.put_object(
        Bucket="insight-flow-project",
        Key=file_path,
        Body=json.dumps(body),
        ContentType="application/json"
    )

    print("Saved CRM event to S3:", file_path)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "CRM webhook received successfully"
        })
    }