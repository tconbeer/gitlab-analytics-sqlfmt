with
    source as (select * from {{ source("xactly", "xc_period_type") }}),

    renamed as (

        select

            period_type_id::float as period_type_id,
            version::float as version,
            name::varchar as name,
            is_active::varchar as is_active,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name

        from source

    )

select *
from renamed
