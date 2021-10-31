with promotions as (
    select * from {{ ref('base_complete_journey__promotions' )}}
),

display_types as (
    select * from {{ ref('display_types') }}
),

mailer_types as (
    select * from {{ ref('mailer_types') }}
),

final as (
    select
        promotions.product_id,
        promotions.store_id,
        promotions.week_number,
        display_types.value as display_type,
        mailer_types.value as mailer_type
    from promotions
    left join display_types on promotions.display_id = display_types.display_id
    left join mailer_types on promotions.mailer_id = mailer_types.mailer_id
)

select * from final