drop table if exists ads_pv_uv_puv;
create table ads_pv_uv_puv(
                              dates string,
                              pv bigint,
                              uv bigint,
                              puv decimal(16,2)
)
    row format delimited fields terminated by '\t'
    LOCATION '/warehouse/taobao/ads/ads_pv_uv_puv/';

insert overwrite table ads_pv_uv_puv
select date_format(create_time, 'yyyy-MM-dd') dates,
       count(behavior) pv,
       count(distinct user_id) uv,
       cast(count(behavior)/count(distinct user_id) as decimal(16, 2)) as puv
from dwd_user_behavior
where behavior = 'pv'
group by dates;
