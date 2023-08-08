drop table if exists ads_sku_rebuy;
create table ads_sku_rebuy(
                              sku_id bigint,
                              `rebuy_rate` string
)
    row format delimited fields terminated by '\t'
    location '/warehouse/taobao/ads/ads_sku_rebuy';
with t1 as (
    select sku_id, user_id, count(*) as buy_count
    from dwd_user_behavior
    where behavior='buy'
    group by sku_id, user_id
),
     t2 as (
         select sku_id, user_id, sum(`if`(buy_count > 0, 1, 0)) buy_one,
                sum(`if`(buy_count > 1, 1, 0)) buy_two
         from t1
         group by sku_id, user_id
     )
insert overwrite table ads_sku_rebuy
select sku_id, concat(round(sum(buy_two)/sum(buy_one)*100, 2), '%') rebuy_rate
from t2
group by sku_id
