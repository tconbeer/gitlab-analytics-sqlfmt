with
    source as (select * from {{ source("xactly", "xc_plan") }}),

    renamed as (

        select

            plan_id::float as plan_id,
            version::float as version,
            name::varchar as name,
            description::varchar as description,
            is_active::varchar as is_active,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            period_id::float as period_id

        from source

    )

select *
from renamed
