"""
Create archives list for Monkeypox reports
"""
import os
import json
import logging
from typing import Any
from pathlib import Path

import boto3
import chevron

BUCKET = os.getenv("WEBSITE_BUCKET", "www.monkeypox.global.health")
TEMPLATE = Path(__file__).parent / "archives.html"


s3 = boto3.resource("s3")


def read_object(obj) -> dict[str, int | str]:
    "Return dictionary from an S3 Object representing JSON data"
    return json.loads(obj.get()["Body"].read().decode("utf-8"))


def keep(dictionary, keys: list[str]):
    "Return dictionary with a subset of keys"
    return {k: dictionary[k] for k in keys}


def fetch_list(bucket_name: str) -> list[dict[str, Any]]:
    "Fetch indices list as index.json from S3 bucket"
    logging.info(f"Fetching list of indices from bucket {bucket_name}")
    bucket = s3.Bucket(bucket_name)
    try:
        indices = sorted(
            [
                obj
                for obj in bucket.objects.all()
                if obj.key.endswith("index.json") and obj.key.startswith("20")  # year
            ],
            key=lambda obj: obj.key,
            reverse=True,
        )
    except Exception:
        logging.error("Error in fetching list of archives")
        raise
    return {
        "archives": [
            keep(
                read_object(obj),
                ["date", "n_confirmed", "n_suspected"],
            )
            for obj in indices
        ]
    }


def render_archives(archive_data: dict[str, Any], template: Path) -> str:
    "Render archives data from template"
    with template.open() as f:
        return chevron.render(f, archive_data)


def upload(bucket: str, body: str):
    "Upload archives data to bucket"
    logging.info(f"Uploading data to {bucket}")
    try:
        s3.Object(bucket, "archives/index.html").put(Body=body, ContentType="text/html")
    except Exception:
        logging.error("Exception when trying to upload archives data")
        raise


if __name__ == "__main__":
    archives_data = fetch_list(BUCKET)
    upload(BUCKET, render_archives(archives_data, TEMPLATE))
