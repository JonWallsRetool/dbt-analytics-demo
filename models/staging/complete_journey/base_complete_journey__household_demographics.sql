with source as (
    select * from {{ source('complete_journey', 'hh_demographic') }}
),

renamed as (

    select
        age_desc as age_range,
        marital_status_code as marital_status_id,
        income_desc as income_range,
        homeowner_desc as home_ownership,
        hh_comp_desc as household_composition,
        household_size_desc as household_size,
        kid_category_desc as number_of_children_range,
        household_key as household_id

    from source

)

select * from renamed