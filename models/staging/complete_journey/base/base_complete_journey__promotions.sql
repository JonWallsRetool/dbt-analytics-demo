with source as (

    select * from {{ source('complete_journey', 'causal_data') }}

),

renamed as (

    select
        product_id as product_id,
        store_id as store_id,
        week_no as week_number,
        display as display_id,
        mailer as mailer_id

    from source

)

select * from renamed