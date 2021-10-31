with source as (

    select * from {{ source('complete_journey', 'transaction_data') }}

),

renamed as (

    select
        household_key as household_id,
        basket_id,
        -- day as day_number,
        date_sub(current_date(), interval ({{ get_demo_duration() }} - day) day) as transaction_date,
        product_id,
        quantity,
        sales_value,
        store_id,
        retail_disc as retail_discount,
        time(cast(floor(trans_time/100) as int64), MOD(trans_time, 100), 00) as transaction_time,
        week_no as week_number,
        coupon_disc as coupon_discount,
        coupon_match_disc as coupon_match_discount

    from source

)

select * from renamed