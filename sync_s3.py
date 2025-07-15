import os
import boto3
import botocore.exceptions
from datetime import datetime, timedelta, timezone
from config import (
    LOCAL_SYNC_DIR,
    AWS_S3_BUCKET,
    AWS_S3_PREFIX,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    SYNC_LOOKBACK_HOURS,
)
from utils import log

# 初始化 S3 客户端
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

now = datetime.now(timezone.utc)
cutoff_time = now - timedelta(hours=SYNC_LOOKBACK_HOURS)

def sync_from_s3():
    log(f"Syncing files modified after: {cutoff_time.isoformat()}")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=AWS_S3_PREFIX)

        for page in page_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]

                if last_modified < cutoff_time:
                    continue
                if not key.endswith(".gz"):
                    continue

                # 获取当天日期字符串
                date_str = last_modified.strftime("%Y-%m-%d")
                local_dir = os.path.join(LOCAL_SYNC_DIR, date_str)
                os.makedirs(local_dir, exist_ok=True)

                filename = os.path.basename(key)
                local_path = os.path.join(local_dir, filename)

                if os.path.exists(local_path):
                    continue  # 已存在，跳过

                log(f"Downloading {key} → {local_path}")
                try:
                    s3.download_file(AWS_S3_BUCKET, key, local_path)
                    log(f"Downloaded: {filename}")
                except Exception as e:
                    log(f"Failed to download {key}: {e}")

    except botocore.exceptions.NoCredentialsError:
        log("ERROR: No AWS credentials found. Please check config.py.")
    except Exception as e:
        log(f"Unexpected error while syncing S3: {e}")
