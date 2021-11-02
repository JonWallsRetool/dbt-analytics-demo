with source as (
    select * from {{ source('complete_journey', 'transaction_data') }}
),

final as (
  select
    household_key as household_id,
    store_id,
    count(distinct basket_id) as store_vists,
    row_number() over (partition by household_key order by count(distinct basket_id) desc) as store_rank

  from source
  group by 1,2
)

select * from final