drop table if exists dws_user_behavior;
create table dws_user_behavior (
   user_id bigint,
   all_count bigint,
   pv_count bigint,
   fav_count bigint,
   cart_count bigint,
   buy_count bigint
)
    row format delimited fields terminated by '\t'
location '/warehouse/taobao/dws/dws_user_behavior';
-- 用户行为汇总
insert overwrite table dws_user_behavior
select user_id, count(behavior) all_count,
       sum(`if`(behavior='pv', 1, 0)) pv_count,
       sum(`if`(behavior='fav', 1, 0)) fav_count,
       sum(`if`(behavior='cart', 1, 0)) cart_count,
       sum(`if`(behavior='buy', 1, 0)) buy_count
from dwd_user_behavior
group by user_id
