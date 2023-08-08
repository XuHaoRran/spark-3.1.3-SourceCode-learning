drop table if exists ads_category_sell_rank;
create table ads_category_sell_rank (
                                        catgegory_name string,
                                        sku_id bigint,
                                        sell_num bigint,
                                        rank bigint
)
    row format delimited fields terminated by '\t'
location '/warehouse/taobao/ads/ads_category_sell_rank';
-- 商品类目销售排行版
insert overwrite table ads_category_sell_rank
select catgegory_name, sku_id, sell_num, rank
from(
        select distinct catgegory_name, sku_id, cnt sell_num, dense_rank() over (partition by catgegory_name order by cnt desc) rank
        from
            (
                select sku_id, catgegory_name, count(sku_id) over (partition by sku_id) cnt
                from dwd_user_behavior
                where behavior='buy'
            ) t1
    ) t2
where rank <= 10;
