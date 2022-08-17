
with
    source as (select * from {{ ref("gitlab_dotcom_plans_dedupe_source") }}),
    renamed as (

        select

            id::number as plan_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            name::varchar as plan_name,
            title::varchar as plan_title,
            id in (2, 3, 4, 100, 101) as plan_is_paid

        from source

    )

select *
from renamed
