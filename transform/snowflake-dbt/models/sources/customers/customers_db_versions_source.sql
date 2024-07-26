{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (

        select *
        from {{ source("customers", "customers_db_versions") }}
        qualify row_number() over (partition by id order by _uploaded_at desc) = 1

    ),
    renamed as (

        select
            id::number as version_id,
            item_id::number as item_id,
            transaction_id::number as transaction_id,
            created_at::timestamp as created_at,
            event::varchar as event,
            item_type::varchar as item_type,
            object::varchar as object,
            object_changes::varchar as object_changes,
            whodunnit::varchar as whodunnit
        from source

    )

select *
from renamed
