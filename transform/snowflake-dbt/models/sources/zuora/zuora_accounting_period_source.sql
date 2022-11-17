with
    source as (select * from {{ source("zuora", "accounting_period") }}),
    renamed as (

        select
            -- Primary Keys
            id::varchar as accounting_period_id,

            -- Info
            enddate::timestamp_tz as end_date,
            fiscalyear::number as fiscal_year,
            name::varchar as accounting_period_name,
            startdate::timestamp_tz as accounting_period_start_date,
            status::varchar as accounting_period_status,
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date

        from source

    )

select *
from renamed
