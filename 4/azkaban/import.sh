#!/bin/bash

# time format: "year-month-date"
time=$(date +"%Y-%m-%d")
sql='load data inpath "/data/'$time'/clicklog.dat" into table homework.user_clicks PARTITION(dt='$time');'
echo $sql > /root/job/import.hql
/opt/lagou/servers/hive-2.3.7/bin/hive -f /root/job/import.hql
