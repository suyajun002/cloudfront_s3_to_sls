import os

# 根目录：所有日志存放到这里，再按日期分目录
LOCAL_SYNC_DIR = "/data/apps/cloudfront_s3_to_sls/data"

# 阿里云日志服务配置
SLS_ENDPOINT = "us-hongkong-1.log.aliyuncs.com"
SLS_PROJECT = "project-log"
SLS_LOGSTORE = "cloudfront-log"
ACCESS_KEY_ID = "xxxxx"
ACCESS_KEY_SECRET = "xxxx"

# s3配置
AWS_ACCESS_KEY_ID = "xxx"
AWS_SECRET_ACCESS_KEY = "xx+xx+xx"
AWS_REGION = "us-hongkong-1"
AWS_S3_BUCKET = "cloudfront-s3"
AWS_S3_PREFIX = ""  # 为空表示根目录

# 控制同步s3上最近的文件时间范围（单位：小时）
SYNC_LOOKBACK_HOURS = 1

# 只处理最近多少小时内的文件到sls（单位：小时）
PROCESS_LOOKBACK_HOURS = 1
