import os
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime

import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By

NEXTSTRAIN_MPXV = "https://nextstrain.org/monkeypox/hmpxv1"


def fetch_metadata(link: str) -> Optional[Path]:
    driver = webdriver.Firefox()
    driver.get(link)

    def find_button(text):
        if not (
            elems := [
                e for e in driver.find_elements(By.TAG_NAME, "button") if e.text == text
            ]
        ):
            raise ValueError(f"No button found with {text}")
        else:
            print(text)
        return elems[0]

    find_button("DOWNLOAD DATA").click()
    find_button("METADATA (TSV)").click()
    if (
        file := Path.home() / "Downloads" / "nextstrain_monkeypox_hmpxv1_metadata.tsv"
    ).exists():
        return file
    return None


def upload(file: Path):
    if file is None:
        logging.error("Nextstrain file not downloaded")
        return None
    if not (BUCKET := os.getenv("MONKEYPOX_BUCKET")):
        raise ValueError("Specify bucket to copy files to in MONKEYPOX_BUCKET")
    s3 = boto3.resource("s3")
    today = datetime.today().date()
    try:
        s3.Object(BUCKET, f"{today}/nextstrain_monkeypox_hmpxv1_metadata.tsv").put(
            Body=file.read_text()
        )
    except Exception as exc:
        logging.exception("Failed to upload Nextstrain metadata")
        raise


if __name__ == "__main__":
    upload(fetch_metadata(NEXTSTRAIN_MPXV))
