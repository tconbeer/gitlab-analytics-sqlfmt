with
    source as (select * from {{ source("netsuite", "entity") }}),
    renamed as (

        select
            -- Primary Key
            entity_id::float as entity_id,

            -- Info
            name::varchar as entity_name,
            full_name::varchar as entity_full_name,
            _fivetran_deleted::boolean as is_fivetran_deleted

        from source

    )

select *
from renamed
