with source as (
    select * from {{ source('complete_journey', 'causal_data') }}
),

renamed as (

    select
        concat(cast(product_id as string), '|', cast(store_id as string), '|', cast(week_no as string)) as pk_promotions,
        product_id as product_id,
        store_id as store_id,
        week_no as week_number,

        array_agg(distinct case when display = '0' then null else display end ignore nulls)[offset(0)] as display_id,
        array_agg(distinct case when mailer = '0' then null else mailer end ignore nulls)[offset(0)] as mailer_id

    from source
    group by 1,2,3,4

)

select * from renamed