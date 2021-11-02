with source as (
  select 
    'transaction_data_uniques' as spine,
    count(*) as row_count
  from 
    {{ source('complete_journey', 'transaction_data') }}
  group by 1
),

warehouse as (
  select 
    'transaction_data_uniques' as spine,
    count(*) as row_count
  from 
    {{ ref('fct_purchased_items') }}
  group by 1
),

mart as (
    select
        'transaction_data_uniques' as spine,
        count(*) as row_count
    from
        {{ ref('mart_purchased_items') }}
    group by 1
),

combined as (
    select spine, row_count as source_rows, NULL as fact_rows, NULL as mart_rows from source
    union all 
    select spine, NULL as source_rows, row_count as fact_rows, NULL as mart_rows from warehouse
    union all
    select spine, NULL as source_rows, NULL as fact_rows, row_count as mart_rows from mart
)

select 
    spine,
    sum(source_rows) as source_rows,
    sum(fact_rows) as fact_rows,
    sum(mart_rows) as mart_rows,
    sum(fact_rows) - sum(source_rows) as source_to_fact,
    sum(mart_rows) - sum(fact_rows) as fact_to_mart
from combined
group by 1
having source_to_fact != 0 and fact_to_mart != 0