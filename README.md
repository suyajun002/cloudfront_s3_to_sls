# cloudfront_s3_to_sls
aws的cloudfront 可以把访问日志采集到s3，aws也有日志查看的功能，但是用起来不不好用，而且每次查看日志的时候还要登陆aws的账号去查看。 考虑到业务日志也在阿里云的sls上，所以就把日志放到sls上做统一展示和查看

- 大致的架构
```
cloudfront -> s3 -> ecs(和sls在同一个地域) -> sls
```

- 依赖信息
```
/root/anaconda3/bin/python3 --version
Python 3.12.11
/root/anaconda3/bin/pip3 install boto3
/root/anaconda3/bin/pip3 install  -U aliyun-log-python-sdk
```

- 项目目录结构
```
ll /data/apps/cloudfront_s3_to_sls/ | awk '{print $NF}'
config.py # 配置文件，记录s3和sls的相关信息
data #保存从s3上同步下来的文件
parse_and_push.py  #同步日志内容到sls
run.py  #主程序
sync_s3.py #从s3上同步日志文件
utils.py   #日记模块
```
- 运行方式
```
crontab -l
*/5 * * * * cd /data/apps/cloudfront_s3_to_sls;/root/anaconda3/bin/python3 run.py >> run.log 2>&1
```
