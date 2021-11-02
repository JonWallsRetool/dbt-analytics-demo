{{ config(
    materialized='table',
    partition_by={
      "field": "household_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 2500,
        "interval": 10
      }
    },
    cluster_by = ["household_id", "latest_purchase", "favourite_store"],
)}}

with transactions as (
    select * from {{ ref('fct_purchased_items') }}
),

demographics as (
    select * from {{ ref('stg_complete_journey__household_demographics') }}
),

household_store_rank as (
    select * from {{ ref('household_stores__ranked') }}
    where store_rank = 1
),

final as (

    select
        transactions.household_id,

        demographics.age_range,
        demographics.marital_status,
        demographics.income_range,
        demographics.home_ownership,
        demographics.household_composition,
        demographics.household_size,
        demographics.number_of_children_range,

        household_store_rank.store_id as favourite_store,

        min(transaction_date) as first_purchase,
        max(transaction_date) as latest_purchase,
        floor(sum(sales_value)) as lifetime_purchase_value,
        count(distinct basket_id) as lifetime_purchase_quantity

    from transactions
    left join demographics on transactions.household_id = demographics.household_id
    left join household_store_rank on transactions.household_id = household_store_rank.household_id
    group by 1,2,3,4,5,6,7,8,9
)

select * from final