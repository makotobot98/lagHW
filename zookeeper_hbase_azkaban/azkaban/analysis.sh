#!/bin/bash
time=$(date +"%Y-%m-%d")

sql="insert into table homework.user_info select count(distinct id), to_date(dt) from homework.user_clicks where to_date(dt) = '$time';"
echo $sql > /root/job/analysis.hql
/opt/lagou/servers/hive-2.3.7/bin/hive -f /root/job/analysis.hql
