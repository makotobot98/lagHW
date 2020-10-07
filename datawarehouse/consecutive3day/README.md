# requirement
1. 在会员分析中计算最近七天连续三天活跃会员数。

2. 项目的数据采集过程中，有哪些地方能够优化，如何实现？

# Part 1
建立两个表: 
1. dws层 `dws_consecutive_3_day`: 该表记录所有过去一周连续三天登录的用户
```sql
create table dws.dws_consecutive_3_day
(
  `uid` string,
  `count` int
) COMMENT 'active members that consecutively logged in more than 3 days in the past week '
partitioned by(dt string)
```
2. ads层 `ads_consecutive_3_day_cnt`: 该表是基于`dws_consecutive_3_day`记录所有过去一周连续三天登录的用户数
```sql
create table ads.ads_consecutive_3_day_cnt
(
  `count` int
) COMMENT 'active members count that consecutively logged in more than 3 days in the past week '
partitioned by(dt string)
```

## load data
- load data的逻辑写在`ads_load_consecutive3.sh`
```bash
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
```

## Part 2
### 1. 调参
- 可以调整flume agent的 `batchSize`和`batchDurationMillis`来提高系统的吞吐量
- 根据需求调整channel类型，性能优先使用memory channel，容错性优先使用file channel
### 2. 加大并向量
- 使用`sink group`来实现sink之间的负载均衡
- 增加并行的`channel`
- 使用负载均衡channel selector(`Round-Robin Channel Selector`)提高channel之间的并行量

### 3. 序列化
- 将日志内容序列化，提高系统吞吐量
