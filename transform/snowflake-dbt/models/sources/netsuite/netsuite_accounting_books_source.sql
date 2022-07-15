with
    source as (select * from {{ source("netsuite", "accounting_books") }}),
    renamed as (

        select
            -- Primary Key
            accounting_book_id::float as accounting_book_id,

            -- Info
            accounting_book_extid::varchar as accounting_book_extid,
            accounting_book_name::varchar as accounting_book_name,
            base_book_id::float as base_book_id,
            date_created::timestamp_tz as date_created,
            date_last_modified::timestamp_tz as date_last_modified,
            effective_period_id::float as effective_period_id,
            form_template_component_id::varchar as form_template_component_id,
            form_template_id::float as form_template_id,
            is_primary::boolean as is_primary,
            status::varchar as status

        from source

    )

select *
from renamed
