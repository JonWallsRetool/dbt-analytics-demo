with source as (

    select * from {{ source('complete_journey', 'product') }}

),

renamed as (

    select
        product_id,
        manufacturer as manufacturer_id,
        department,
        brand,
        commodity_desc as commodity_description,
        sub_commodity_desc as sub_commodity_description,
        curr_size_of_product as current_size_of_product

    from source

)

select * from renamed