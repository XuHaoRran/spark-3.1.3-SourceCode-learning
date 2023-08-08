drop table if exists ads_date_hour_behavior;
create table ads_date_hour_behavior(
   date string,
   hour string,
   pv bigint,
   cart bigint,
   fav bigint,
   buy bigint
)
    row format delimited fields terminated by '\t'
LOCATION '/warehouse/taobao/ads/ads_date_hour_behavior/';

insert into ads_date_hour_behavior
select date_format(create_time, 'yyyy-MM-dd'), hour(create_time),
    sum(`if`(behavior='pv', 1, 0)) pv,
    sum(`if`(behavior='cart', 1, 0)) cart,
    sum(`if`(behavior='fav', 1, 0)) fav,
    sum(`if`(behavior='buy', 1, 0)) buy
from dwd_user_behavior
group by date_format(create_time, 'yyyy-MM-dd'), hour(create_time)
order by date_format(create_time, 'yyyy-MM-dd'), hour(create_time);
