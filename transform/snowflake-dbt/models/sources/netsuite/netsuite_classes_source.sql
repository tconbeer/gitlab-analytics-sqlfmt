with
    source as (select * from {{ source("netsuite", "classes") }}),
    renamed as (

        select
            -- Primary Key
            class_id::float as class_id,

            -- Info
            name::varchar as class_name,
            full_name::varchar as class_full_name,
            isinactive::boolean as is_inactive,
            _fivetran_deleted::boolean as is_fivetran_deleted

        from source

    )

select *
from renamed
