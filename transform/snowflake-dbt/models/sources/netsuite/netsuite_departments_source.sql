with
    source as (select * from {{ source("netsuite", "departments") }}),
    renamed as (

        select
            -- Primary Key
            department_id::float as department_id,

            -- Foreign Key
            parent_id::float as parent_department_id,

            -- Info
            name::varchar as department_name,
            full_name::varchar as department_full_name,

            -- Meta
            isinactive::boolean as is_department_inactive

        from source

    )

select *
from renamed
