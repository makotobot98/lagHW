# 1

用拉链表实现核心交易分析中DIM层商家维表，并实现该拉链表的回滚

### 1 创建表商铺拉链维表

```sql
DROP TABLE IF EXISTS `dim.dim_trade_shops`;
CREATE TABLE `dim.dim_trade_shops` (
`shopid` int,
`userid` int,
`areaid` int,
`shopname` string,
`shoplevel` tinyint,
`status` tinyint,
`createtime` string,
`modifytime` string,
`start_time` string,
`end_time` string
) COMMENT '商家店铺维表'
STORED AS PARQUET;
```

### 2 初始加载数据, 初始日期为传入参数的日期

```sql
insert overwrite table dim.dim_trade_shops
select shopid, userid, areaid, shopname, shoplevel, status, createtime, modifytime, 
		case when modifyTime is not null
			 then substr(modifyTime, 0, 10)
			 else substr(createTime, 0, 10)
		end as start_time,
		'9999-12-32' as end_time
from ods.ods_trade_shops
where dt = '$do_date'
```

### 3 增量加载数据(新增数据union已有数据)

```sql
insert overwrite table dim.dim_trade_shops
select shopid, userid, areaid, shopname, shoplevel, status, createtime, modifytime, 
		case when modifyTime is not null
			 then substr(modifyTime, 0, 10)
			 else substr(createTime, 0, 10)
		end as start_time,
		'9999-12-32' as end_time
from ods.ods_trade_shops
where dt = '$do_date'
union all
insert overwrite table dim.dim_trade_shops
select dim.shopid, dim.userid, dim.areaid, dim.shopname, dim.shoplevel, dim.status, dim.createtime, dim.modifytime, dim.start_time, case when dim.end_time >= '9999-12-31' and ods.shopid is not null then '$do_date' else dim.end_time end as end_time
from dim.dim_trade_shops dim left join (select * from ods.ods_trade_shops where dt = '$do_date') ods
where dim.shopid = ods.shopid
```

### 4. 商铺维表拉链回滚实现

回滚到`回滚日期`**以前**的数据, 回滚数据应满足任意以下两个条件:

1. `end_time` < `回滚日期`, 也就是混滚后依然存活的老数据
2. `start_time` <= `回滚日期` AND `end_time` >= 回滚日期，也就是回滚后最新的数据

```sql
select dim.shopid, dim.userid, dim.areaid, dim.shopname, dim.shoplevel, dim.status, dim.createtime, dim.modifytime, dim.start_time, dim.end_time
from dim.dim_trade_shops
where dim.end_time < '$do_date'
union all
select dim.shopid, dim.userid, dim.areaid, dim.shopname, dim.shoplevel, dim.status, dim.createtime, dim.modifytime, dim.start_time, '9999-12-31' as end_time
from dim.dim_trade_shops
where dim.start_time <= '$do_date' and dim.end_time >= '$do_date'
```

# 2

### 1. 沉默会员数

沉默会员的定义：只在安装当天启动过App，而且安装时间是在7天前

- 思路：从dws_member_start_week中选出过去6天内的活跃用户，从dws_member_add_day中选出1周前称为新会员的会员；
- `dws_member_add_day` left join `dws_member_start_week` on uid, 如果右边的uid = null则说明这个用户一周前成为新会员并在接下来这一周(除了成为新会员那一天)都没有登录，也就是沉默会员

```sql
-- 创建表
drop table if exists ads.ads_silence_member_count;
create table ads.ads_silence_member_count(
`silence_member_cnt` int
)
partitioned by(dt string)
row format delimited fields terminated by ',';
```



```sql
insert overwrite table ads.ads_silence_member_count
partition(dt='$do_date')
select count(distinct *) as silence_member_cnt
(
    select add_day.uid
    from (select uid from dws.dws_member_start_week where dt = '$do_date') add_day 
    	 left join 
    	 (select uid from dws.dws_member_start_week where dt >= date_add('$do_date', -6) and dt <= '$do_date') start_week
    where start_week.uid is null
);

```



### 2. 流失会员数

流失会员的定义：最近30天未登录的会员

```sql
-- 创建表
drop table if exists ads.ads_lost_member_count;
create table ads.ads_lost_member_count(
`lost_member_cnt` int
)
partitioned by(dt string)
row format delimited fields terminated by ',';
```



```sql
insert overwrite table ads.ads_lost_member_count
partition(dt='$do_date')
select count(distinct *) as lost_member_cnt
from (
    select uid 
    from (select uid from dws.dws_member_start_day where dt < date_add('$do_date', -30)) t
    where t.uid NOT IN (select uid from dws.dws_member_start_month where dt = '$do_date')
);
```



# 3

在核心交易分析中完成如下指标的计算

1. 统计2020年每个季度的销售订单笔数、订单总额

2. 统计2020年每个月的销售订单笔数、订单总额

3. 统计2020年每周（周一到周日）的销售订单笔数、订单总额

4. 统计2020年国家法定节假日、休息日、工作日的订单笔数、订单总额

```sql
-- 创建表
DROP TABLE IF EXISTS ads.ads_trade_order_analysis_bydate
create table if not exist ads.ads_trade_order_analysis_bydate(
    date_type string,
    season int,
    month string,
    week string,
    holiday string,
    order_cnt int,
    order_amount int
)
partitioned by (dt string)
row format delimited fields terminated by ',';
-- 创建holiday表，用来存2020国家法定节假日，休息日，工作日的日期, 节假日写在'holiday.dat'文本中，将数据load到holiday table中
DROP TABLE IF EXISTS tmp.tmp_holidays
CREATE TABLE IF NOT EXISTS tmp.tmp_holidays(
	dt string
)
row format delimited fields terminated by ',';
```



```sql
-- 加载数据, 传入参数'$do_year'为需要统计的年度

-- 从hdfs路径下加载holiday表
load data inpath 'data/holiday.dat' overwrite into table tmp.tmp_holidays;

-- 创建tmp表选出今年的订单数据, 由于相同订单会出现多次，还需要进行去重
with order_year as (
	select orderid, max(paymoney) as paymoney, max(dt) as dt
    from dws.dws_trade_orders
    where year(dt) >= '$do_year'
    group by orderid
)

-- 1. 统计2020年每个季度的销售订单笔数、订单总额, 使用floor(month / 3.1) + 1来算季度
insert overwrite table ads.ads_trade_order_analysis_bydate
partition(dt='$do_year')
select 'season' as date_type, floor(month(t.dt) / 3.1) + 1 as season, '' as month, '' as week, '' as holiday, count(1) as order_cnt, sum(t.paymoney) as order_amount
from order_year t
group by floor(month(t.dt) / 3.1) + 1

-- 2. 统计2020年每个月的销售订单笔数、订单总额
union all
select 'month' as date_type, '' as season, month(t.dt) as month, '' as week, '' as holiday, count(1) as order_cnt, sum(t.paymoney) as order_amount
from order_year t
group by month(t.dt)

-- 3. 统计2020年每周（周一到周日）的销售订单笔数、订单总额
union all
select 'week' as date_type, '' as season, '' as month, date_add(next_day(dt, 'mo'), -7) as week, '' as holiday, count(1) as order_cnt, sum(t.paymoney) as order_amount
from order_year t
group by date_add(next_day(dt, 'mo'), -7)

-- 4. 统计2020年国家法定节假日、休息日、工作日的订单笔数、订单总额
union all
select 'holiday' as date_type, '' as season, '' as month, '' as week, '' as holiday, count(1) as order_cnt, sum(t.paymoney) as order_amount
from order_year t
where t.dt in (select * from tmp.tmp_holidays)
group by t.dt;
```

