with
    source as (select * from {{ source("netsuite", "subsidiaries") }}),
    renamed as (

        select
            -- Primary Key
            subsidiary_id::float as subsidiary_id,

            -- Info
            full_name::varchar as subsidiary_full_name,
            name::varchar as subsidiary_name,
            base_currency_id::float as base_currency_id,
            fiscal_calendar_id::float as fiscal_calendar_id,
            parent_id::float as parent_id,

            -- Meta
            isinactive::boolean as is_subsidiary_inactive,
            is_elimination::boolean as is_elimination_subsidiary

        from source

    )

select *
from renamed
