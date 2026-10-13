import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

SECRET_NAME = "insight-flow/wistia-api-token"
BUCKET_NAME = "insight-flow-project"
CHECKPOINT_KEY = "bronze/wistia/_checkpoint/latest_received_at.txt"

MEDIA_IDS = [
    "8hunphufxp",
    "9k4tbcdfg0"
]

secrets_client = boto3.client("secretsmanager")
s3 = boto3.client("s3")


def get_wistia_token():
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["WISTIA_API_TOKEN"]


def get_checkpoint():
    try:
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=CHECKPOINT_KEY
        )
        return response["Body"].read().decode("utf-8").strip()

    except ClientError as e:
        if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
            return None
        raise


def save_checkpoint(received_at):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=CHECKPOINT_KEY,
        Body=received_at,
        ContentType="text/plain"
    )


def call_wistia_api(url, token):
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "X-Wistia-API-Version": "2026-09"
        }
    )

    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def lambda_handler(event, context):

    token = get_wistia_token()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

    # -----------------------------
    # 1. WISTIA EVENTS API
    # -----------------------------

    checkpoint = get_checkpoint()

    end_date = datetime.now(timezone.utc).date()

    if checkpoint:
        checkpoint_dt = datetime.fromisoformat(
            checkpoint.replace("Z", "+00:00")
        )
        start_date = checkpoint_dt.date()
    else:
        start_date = end_date - timedelta(days=1)

    page = 1
    per_page = 100
    new_events = []

    while True:

        params = urllib.parse.urlencode({
            "api_password": token,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page": page,
            "per_page": per_page
        })

        url = f"https://api.wistia.com/v1/stats/events.json?{params}"

        request = urllib.request.Request(url)

        with urllib.request.urlopen(request) as response:
            events = json.loads(response.read().decode("utf-8"))

        if not events:
            break

        for wistia_event in events:

            received_at = wistia_event.get("received_at")

            if not received_at:
                continue

            if checkpoint is None or received_at > checkpoint:
                new_events.append(wistia_event)

        if len(events) < per_page:
            break

        page += 1

    event_key = None

    if new_events:

        event_key = f"bronze/wistia/events/wistia_events_{timestamp}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=event_key,
            Body=json.dumps(new_events),
            ContentType="application/json"
        )

        latest_received_at = max(
            item["received_at"]
            for item in new_events
            if item.get("received_at")
        )

        save_checkpoint(latest_received_at)

    # -----------------------------
    # 2. WISTIA MEDIA API
    # -----------------------------

    media_records = []

    for media_id in MEDIA_IDS:

        url = (
            f"https://api.wistia.com/modern/medias/{media_id}"
        )

        media = call_wistia_api(url, token)
        media_records.append(media)

    media_key = f"bronze/wistia/media/wistia_media_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=media_key,
        Body=json.dumps(media_records),
        ContentType="application/json"
    )

    # -----------------------------
    # 3. WISTIA MEDIA STATS API
    # -----------------------------

    stats_records = []

    # One timestamp represents this entire Stats snapshot
    snapshot_timestamp = datetime.now(timezone.utc).isoformat()

    for media_id in MEDIA_IDS:

        url = (
            f"https://api.wistia.com/modern/stats/medias/{media_id}"
        )

        stats = call_wistia_api(url, token)

        stats_records.append({
            "media_id": media_id,
            "snapshot_timestamp": snapshot_timestamp,
            "stats": stats
        })

    stats_key = f"bronze/wistia/stats/wistia_stats_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=stats_key,
        Body=json.dumps(stats_records),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "new_event_records": len(new_events),
            "event_s3_key": event_key,
            "media_records": len(media_records),
            "media_s3_key": media_key,
            "stats_records": len(stats_records),
            "stats_s3_key": stats_key
        })
    }