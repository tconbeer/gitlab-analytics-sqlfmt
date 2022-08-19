with
    source as (

        select * from {{ source("zuora_central_sandbox", "accounting_period") }}

    ),
    renamed as (

        select
            -- Primary Keys
            id::varchar as accounting_period_id,

            -- Info
            end_date::timestamp_tz as end_date,
            fiscal_year::number as fiscal_year,
            name::varchar as accounting_period_name,
            start_date::timestamp_tz as accounting_period_start_date,
            status::varchar as accounting_period_status,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date

        from source

    )

select *
from renamed
