import json
import os
import requests
import boto3
from datetime import datetime

# AWS client
s3 = boto3.client("s3")

# Environment variables
ADZUNA_APP_ID = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]
S3_BUCKET = "adzuna-etl-project-g2"

BASE_URL = "https://api.adzuna.com/v1/api/jobs"

PAGES = 5
RESULTS_PER_PAGE = 50
KEYWORD = "data engineer"
COUNTRY = "ca"


def fetch_adzuna_jobs():
    all_results = []

    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": KEYWORD,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json"
    }

    for page in range(1, PAGES + 1):
        url = f"{BASE_URL}/{COUNTRY}/search/{page}"

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        all_results.extend(data.get("results", []))

    return all_results


def save_to_s3(jobs):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"adzuna/data_engineer_ca/jobs_{timestamp}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(jobs, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )

    return s3_key


def lambda_handler(event, context):
    try:
        jobs = fetch_adzuna_jobs()
        s3_key = save_to_s3(jobs)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Jobs fetched and stored successfully",
                "jobs_fetched": len(jobs),
                "s3_key": s3_key
            })
        }

    except Exception as error:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(error)
            })
        }
