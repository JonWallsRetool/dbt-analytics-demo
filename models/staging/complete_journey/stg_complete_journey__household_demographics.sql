with demographics as (
    select * from {{ ref('base_complete_journey__household_demographics') }}
),

marital_status as (
    select * from {{ ref('marital_status') }}
),

renamed as (

    select
        demographics.age_range as age_range,
        marital_status.value as marital_status,
        demographics.income_range as income_range,
        demographics.home_ownership as home_ownership,
        demographics.household_composition as household_composition,
        demographics.household_size as household_size,
        demographics.number_of_children_range as number_of_children_range,
        demographics.household_id as household_id

    from demographics
    left join marital_status on demographics.marital_status_id = marital_status.marital_status_id

)

select * from renamed