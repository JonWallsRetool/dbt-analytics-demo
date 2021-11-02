{{ config(
    partition_by = {
      "field": "transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["store_id", "department", "brand", "commodity_description"],
)}}

with transactions as (
    select * from {{ ref('fct_purchased_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

households as (
    select * from {{ ref('dim_households')}}
),

promotions as (
    select * from {{ ref('fct_promotions') }}
),

final as (
    select

        transactions.pk_purchased_items,
        transactions.household_id,
        transactions.basket_id,
        transactions.days_ago,
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

        products.manufacturer_id,
        products.department,
        products.brand,
        products.commodity_description,
        products.sub_commodity_description,
        products.current_size_of_product,

        households.age_range,
        households.marital_status,
        households.income_range,
        households.home_ownership,
        households.household_composition,
        households.household_size,
        households.number_of_children_range,
        households.favourite_store,
        households.first_purchase,
        households.latest_purchase,
        households.lifetime_purchase_value,
        households.lifetime_purchase_quantity,

        -- array_agg(distinct promotions.display_type ignore nulls) as display_types,
        -- array_agg(distinct promotions.mailer_type ignore nulls) as mailer_types,
        promotions.display_type,
        promotions.mailer_type

    from transactions
    left join products on transactions.product_id = products.product_id
    left join promotions on 
        transactions.product_id = promotions.product_id
        AND transactions.store_id = promotions.store_id
        AND transactions.week_number = promotions.week_number
    left join households on transactions.household_id = households.household_id

    -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
)

select * from final