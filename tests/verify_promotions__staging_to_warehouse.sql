with staging as (
  select 
    'promotion_uniques' as spine,
    count(distinct pk_promotions) as unique_promotions,
    count(*) as row_count
  from 
    {{ ref('stg_complete_journey__promotions') }}
  group by 1
),

warehouse as (
  select 
    'promotion_uniques' as spine,
    count(*) as row_count
  from 
    {{ ref('fct_promotions') }}
  group by 1
),

combined as (
    select spine, unique_promotions, row_count as staging_rows, NULL as warehouse_rows from staging
    union all 
    select spine, NULL as unique_promotions, NULL as staging_rows, row_count as warehouse_rows from warehouse
)

select 
    spine,
    sum(unique_promotions) as unique_promotions,
    sum(staging_rows) as staging_rows,
    sum(warehouse_rows) as warehouse_rows,
    sum(warehouse_rows) - sum(staging_rows) as staging_to_warehouse,
    sum(warehouse_rows) - sum(unique_promotions) as source_uniques_vs_warehouse
from combined
group by 1
having source_uniques_vs_warehouse != 0