-- 1

with tmp as(
select date_sub(year, row_number() over(partition by team order by year)) year_diff, team
from t1
) 

select team, count(*) count
from tmp
group by tmp.team, tmp.year_diff
having count(*) >= 3;

-- 2
with tmp as (
  select id, time, price, lag(price) over(partition by id order by time) lag, lead(price) over(partition by id order by time) lead
  from t2
)

select id, time, price, case when price > lag and price > lead then 'max'
                             else 'min' end feature
from tmp
where (price > lag and price < lead) or (price < lag and price > lead);

-- 3.1
select tmp.id, (max(tmp.time) - min(tmp.time)) / 60 duration, count(1) count
from (select id, unix_timestamp(dt, 'yyyy/MM/dd hh:mm') time, 1 from t3) tmp
group by tmp.id

-- 3.2
/*
1.使用lag()函数求出dt前一行的值，如果是第一行则使用本身，并且根据id分组 根据dt正序
2.将每一行dt与前一行dt转为时间戳进行相减，然后除以60转换为分钟数
3.使用casewhen 语句，大于30的则为1 小于30 的则为0
4.将结果作为子查询，然后在外面加一层查询，并且使用sum() 作为窗口函数，根据id分组，dt排序（当加了order by 后sum是计算的第一行到当前行的累加）
5.然后再去sum()每条记录的时长，，然后count()计算出步长，并且使用group by 以id 和前面的分组字段进行分组
*/
---- 通过计算dt - lag计算当前date的分区，如果(dt-lag)/60则为新的分区，计算完分区后给表分区进行汇总得出结果
with tmp as (
  select id, unix_timestamp(dt, 'yyyy/MM/dd hh:mm') time, unix_timestamp(lag(dt) over(partition by id order by dt), 'yyyy/MM/dd hh:mm') ptime
  from t3
),

with tmp2 as (  
  select t.id id, t.duration duration, sum(t.label) over(partition by t.id order by t.dt rows between current row and unbounded preceding) partition_label
  from (
    select id, (tmp.time - tmp.ptime)/60 duration, case when (tmp.time - tmp.ptime)/60 >= 30 then 1 else 0 end label
  ) t
)

select id, sum(duration) total_duration, count(*) count
from tmp2
group by tmp2.id, tmp2.label














