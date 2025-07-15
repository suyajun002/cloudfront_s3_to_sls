import time
from sync_s3 import sync_from_s3
from parse_and_push import parse_and_push
from utils import log

def main():
    log("=== Start syncing from S3 ===")
    sync_from_s3()

    log("=== Start parsing and pushing to SLS ===")
    parse_and_push()

    time.sleep(10)

if __name__ == "__main__":
    main()
