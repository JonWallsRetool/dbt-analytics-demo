with demographics as (
    select * from {{ ref('base_complete_journey__hh_demographic') }}
),

transactions as (
    select * from {{ ref('base_complete_journey__transaction_data') }}
),

stage_table as (
    select 
        transactions.household_id,
        transactions.basket_id,
        transactions.day_number,
        transactions.product_id,
        transactions.quantity,
        transactions.sales_value,
        transactions.store_id,
        transactions.retail_discount,
        transactions.transaction_time,
        transactions.week_number,
        transactions.coupon_discount,
        transactions.coupon_match_discount,
        demographics.age_range,
        demographics.marital_status_code,
        demographics.income_range,
        demographics.home_ownership,
        demographics.household_composition,
        demographics.household_size,
        demographics.number_of_children_range
        -- demographics.household_id -- removed as redundant

    from transactions
    left join demographics on transactions.household_id = demographics.household_id
)

select * from stage_table