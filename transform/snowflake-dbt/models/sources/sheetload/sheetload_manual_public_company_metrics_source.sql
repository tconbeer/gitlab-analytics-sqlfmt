with
    source as (

        select
            month::date as month,
            quarter::varchar as quarter,
            year::varchar as year,
            metric_name::varchar as metric_name,
            amount::float as amount,
            created_by::varchar as created_by,
            created_date::date as created_date,
            updated_by::varchar as updated_by,
            updated_date::date as updated_date
        from {{ source("sheetload", "manual_public_company_metrics") }}

    )

select *
from source
