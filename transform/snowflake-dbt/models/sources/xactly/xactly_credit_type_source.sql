with
    source as (select * from {{ source("xactly", "xc_credit_type") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            credit_type_id::float as credit_type_id,
            descr::varchar as descr,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            name::varchar as name

        from source

    )

select *
from renamed
