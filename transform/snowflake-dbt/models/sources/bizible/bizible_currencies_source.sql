with
    source as (

        select
            id as currency_id,
            is_corporate as is_corporate,
            is_enabled as is_enabled,
            modified_date as modified_date,
            modified_date_crm as modified_date_crm,
            created_date as created_date,
            created_date_crm as created_date_crm,
            iso_code as iso_code,
            iso_numeric as iso_numeric,
            exponent as exponent,
            name as name,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_currencies") }}

    )

select *
from source
