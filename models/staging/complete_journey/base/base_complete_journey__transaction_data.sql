with source as (

    select * from {{ source('complete_journey', 'transaction_data') }}

),

renamed as (

    select
        household_key as household_id,
        basket_id,
        day as day_number,
        product_id,
        quantity,
        sales_value,
        store_id,
        retail_disc as retail_discount,
        trans_time as transaction_time,
        week_no as week_number,
        coupon_disc as coupon_discount,
        coupon_match_disc as coupon_match_discount

    from source

)

select * from renamed