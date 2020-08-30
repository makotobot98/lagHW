-- 1. 计算同一个用户上一个click时间lag
with tmp as (
  select user_id id, click_time time, unix_timestamp(lag(click_time) over(partition by user_id order by click_time), 'yyyy-MM-dd hh:mm:ss') ptime
  from user_clicklog
)
-- 2. 计算同用户当前和上一个click时间的差，如果差大于30分钟，添加分区标记；根据分区标记计算哪一些点击属于同一个分区(sum(label) + 1)， 输出结果
select t.id, time, sum(t.label) over(partition by t.id order by t.time rows between unbounded preceding and current row) + 1 label
from (
  select tmp.id, tmp.time, case when (unix_timestamp(tmp.time, 'yyyy-MM-dd hh:mm:ss') - tmp.ptime)/60 >= 30 then 1 else 0 end label
  from tmp
) t;