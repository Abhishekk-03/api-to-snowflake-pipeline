
import json
import boto3
import urllib.request
import uuid
from datetime import datetime, timezone


s3 = boto3.client(
    "s3",
    # aws_access_key_id="",
    # aws_secret_access_key="",
    # region_name="ap-south-1"
)
API_URL = "https://randomuser.me/api/?results=5"


def lambda_handler(event, context):

    # 1. Call the public API
    request = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "AWS-Lambda-DataPipeline"}
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        api_data = json.loads(response.read().decode("utf-8"))

    # 2. Generate a unique run ID
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # 3. Prepare the raw payload
    payload = {
        "run_id": run_id,
        "ingestion_timestamp": now.isoformat(),
        "source": "Random User API",
        "record_count": len(api_data["results"]),
        "data": api_data
    }

    # 4. Create a unique S3 object key 
    s3_key = (
        f"raw/ingestion_date={now:%Y-%m-%d}/"
        f"run_{run_id}.json"
    )

    # 5. Upload JSON to S3
    s3.put_object(
        Bucket="my-api-bronze-data-2026",
        Key=s3_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )

    # 6. Return execution details

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "API data stored in S3",
            "run_id": run_id,
            "records_fetched": len(api_data["results"]),
            "s3_key": s3_key
        })
    }