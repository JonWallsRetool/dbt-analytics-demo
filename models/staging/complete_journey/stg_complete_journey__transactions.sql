with demographics as (
    select * from {{ ref('base_complete_journey__hh_demographic') }}
),

marital_status_codes as (
    select * from {{ ref('marital_status') }}
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
        -- demographics.marital_status_code,
        marital_status_codes.value as marital_status,
        demographics.income_range,
        demographics.home_ownership,
        demographics.household_composition,
        demographics.household_size,
        demographics.number_of_children_range
        -- demographics.household_id -- removed as redundant

    from transactions
    left join demographics on transactions.household_id = demographics.household_id
    inner join marital_status_codes on demographics.marital_status_code = marital_status_codes.marital_status_code
)

select * from stage_table