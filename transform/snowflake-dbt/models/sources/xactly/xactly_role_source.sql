with
    source as (select * from {{ source("xactly", "xc_role") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            descr::varchar as descr,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            name::varchar as name,
            role_id::float as role_id,
            role_type::varchar as role_type

        from source

    )

select *
from renamed
