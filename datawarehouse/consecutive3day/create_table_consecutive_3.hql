
-- dws.dws_consecutive_3_day
drop table if exists dws.dws_consecutive_3_day;
create table dws.dws_consecutive_3_day
(
  `uid` string,
  `count` int
) COMMENT 'active members that consecutively logged in more than 3 days in the past week '
partitioned by(dt string)
stored as parquet;

-- ads.ads_consecutive_3_day_cnt
drop table if exists ads.ads_consecutive_3_day_cnt;
create table ads.ads_consecutive_3_day_cnt
(
  `count` int
) COMMENT 'active members count that consecutively logged in more than 3 days in the past week '
partitioned by(dt string)
stored as parquet;