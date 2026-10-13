import json
import boto3
import urllib.request
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

SOURCE_INDEX_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/file_index.json"
)

SOURCE_BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/"
)

DESTINATION_BUCKET = "insight-flow-project"
DESTINATION_PREFIX = "bronze/marketing_spend/"


def lambda_handler(event, context):

    # 1. Read available files from the public S3 index
    with urllib.request.urlopen(SOURCE_INDEX_URL) as response:
        file_index = json.loads(response.read().decode("utf-8"))

    print("File index:", file_index)

    # 2. Extract filenames
    if isinstance(file_index, list):
        filenames = file_index

    elif isinstance(file_index, dict):
        filenames = (
            file_index.get("files")
            or file_index.get("file_names")
            or file_index.get("filenames")
            or []
        )

    else:
        filenames = []

    # 3. Build yesterday's expected filename
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

    expected_file = f"spend_data_{yesterday}.json"

    print("Expected spend file:", expected_file)

    # 4. Verify yesterday's file exists
    if expected_file not in filenames:
        raise Exception(
            f"{expected_file} is not available in file_index.json"
        )

    # 5. Download yesterday's spend data
    source_url = SOURCE_BASE_URL + expected_file

    with urllib.request.urlopen(source_url) as response:
        spend_data = json.loads(response.read().decode("utf-8"))

    print("Records downloaded:", len(spend_data))

    # Keep only yesterday's records
    spend_data = [
        record
        for record in spend_data
        if record.get("date") == str(yesterday)
    ]

    print("Records for yesterday:", len(spend_data))

    # 6. Store raw JSON in our Bronze layer
    s3_key = DESTINATION_PREFIX + expected_file

    s3.put_object(
        Bucket=DESTINATION_BUCKET,
        Key=s3_key,
        Body=json.dumps(spend_data),
        ContentType="application/json"
    )

    print(
        f"Successfully wrote "
        f"s3://{DESTINATION_BUCKET}/{s3_key}"
    )

    return {
        "statusCode": 200,
        "source_file": expected_file,
        "destination": f"s3://{DESTINATION_BUCKET}/{s3_key}",
        "record_count": len(spend_data)
    }