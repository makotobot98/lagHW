#!/bin/bash
if [ -n "$1" ]
then
  do_date=$1
else
  do_date=`date -d "-1 day" +%F`
fi

sql="
with tmp as (
  select uid, date_sub(dt, row_number() over(partition by uid order by dt)) date_diff
  from dws.dws_member_start_day
  where dt > date_sub(dt, 7) and dt <= '$do_date'
)

insert overwrite table dws.dws_consecutive_3_day
partition(dt='$do_date')
select uid, count(*) count, '$do_date' dt
from tmp
group by uid, date_diff
having count(*) >= 3

insert overwrite table ads.ads_consecutive_3_day_cnt
partition(dt='$do_date')
select count(distint uid)
from dws.dws_consecutive_3_day
where dt = '$do_date'
"

hive -e "$sql"