import os
import gzip
import time
from datetime import datetime, timedelta
from aliyun.log import LogClient, LogItem, PutLogsRequest
from config import (
    LOCAL_SYNC_DIR,
    SLS_ENDPOINT,
    SLS_PROJECT,
    SLS_LOGSTORE,
    ACCESS_KEY_ID,
    ACCESS_KEY_SECRET,
    PROCESS_LOOKBACK_HOURS,
)
from utils import log

client = LogClient(SLS_ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET)

def parse_line_to_log_item(line, fields, source_file):
    if not line.strip() or line.startswith("#"):
        return None
    parts = line.strip().split("\t")
    if len(parts) != len(fields):
        return None

    # 字段名和对应值映射，字段名去括号
    field_map = {
        k.replace("(", "").replace(")", ""): v
        for k, v in zip(fields, parts)
    }

    contents = []
    # 先放 date 和 time
    for key in ("date", "time"):
        if key in field_map:
            contents.append((key, field_map[key]))

    # 再放其他字段，排除 date 和 time
    for k in fields:
        k_clean = k.replace("(", "").replace(")", "")
        if k_clean not in ("date", "time"):
            contents.append((k_clean, field_map[k_clean]))

    # 添加来源文件名字段
    contents.append(("filename", source_file))

    timestamp = int(time.time())
    return LogItem(timestamp, 0, contents)

def send_logs_to_sls(log_items, topic="cloudfront", source="cloudfront_s3"):
    if not log_items:
        return
    try:
        req = PutLogsRequest(
            SLS_PROJECT,
            SLS_LOGSTORE,
            topic,
            source,
            log_items
        )
        res = client.put_logs(req)
        res.log_print()
    except Exception as e:
        log(f"SLS 上传失败: {e}")

def parse_and_push():
    marker_dir = os.path.join(LOCAL_SYNC_DIR, ".processed_files")
    os.makedirs(marker_dir, exist_ok=True)
    marker_file = os.path.join(marker_dir, f"{time.strftime('%Y-%m-%d')}.txt")

    processed_files = set()
    if os.path.exists(marker_file):
        with open(marker_file, "r") as f:
            processed_files = set(line.strip() for line in f)

    now = datetime.now()
    cutoff = now - timedelta(hours=PROCESS_LOOKBACK_HOURS)

    for root, _, files in os.walk(LOCAL_SYNC_DIR):
        for filename in files:
            filepath = os.path.join(root, filename)

            if '.processed_files' in filepath or filepath in processed_files:
                continue

            # 文件修改时间判断
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            if mtime < cutoff:
                continue

            log(f"Processing {filepath}")
            try:
                opener = gzip.open if filepath.endswith(".gz") else open
                with opener(filepath, "rt", encoding="utf-8", errors="ignore") as f:
                    fields = []
                    for line in f:
                        if line.startswith("#Fields:"):
                            fields = line.replace("#Fields:", "").strip().split()
                            break
                    if not fields:
                        log(f"No #Fields found in {filepath}, skipping.")
                        continue
                    f.seek(0)

                    batch = []
                    # 相对于根目录的文件名作为字段值
                    relative_path = os.path.relpath(filepath, LOCAL_SYNC_DIR)

                    for line in f:
                        item = parse_line_to_log_item(line, fields, relative_path)
                        if item:
                            batch.append(item)
                        if len(batch) >= 100:
                            send_logs_to_sls(batch)
                            batch = []
                    if batch:
                        send_logs_to_sls(batch)

                with open(marker_file, "a") as f:
                    f.write(filepath + "\n")
                processed_files.add(filepath)

            except Exception as e:
                log(f"Failed to process {filepath}: {e}")
