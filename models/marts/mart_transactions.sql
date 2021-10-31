{{ config(
    materialized='table',
    partition_by={
      "field": "transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["store_id", "department", "brand", "commodity_description"],
)}}

with products as (
    select * from {{ ref('stg_complete_journey__products') }}
),

promotions as (
    select * from {{ ref('stg_complete_journey__promotions') }}
),

transactions as (
    select * from {{ ref('stg_complete_journey__transactions') }}
),

final as (
    select
        -- products.product_id,
        products.manufacturer_id,
        products.department,
        products.brand,
        products.commodity_description,
        products.sub_commodity_description,
        products.current_size_of_product,

        -- promotions.product_id,
        -- promotions.store_id,
        -- promotions.week_number,
        promotions.display_type,
        promotions.mailer_type,

        transactions.household_id,
        transactions.basket_id,
        transactions.transaction_date,
        transactions.product_id,
        transactions.quantity,
        transactions.sales_value,
        transactions.store_id,
        transactions.retail_discount,
        transactions.transaction_time,
        transactions.week_number,
        transactions.coupon_discount,
        transactions.coupon_match_discount,
        transactions.age_range,
        transactions.marital_status,
        transactions.income_range,
        transactions.home_ownership,
        transactions.household_composition,
        transactions.household_size,
        transactions.number_of_children_range

    from transactions
    left join products on transactions.product_id = products.product_id
    left join promotions on 
        transactions.product_id = promotions.product_id
        AND transactions.store_id = promotions.store_id
        AND transactions.week_number = promotions.week_number
)

select * from final