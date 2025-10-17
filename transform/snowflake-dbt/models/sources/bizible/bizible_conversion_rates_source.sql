with
    source as (

        select
            id as conversion_rate_id,
            currency_id as currency_id,
            source_iso_code as source_iso_code,
            start_date as start_date,
            end_date as end_date,
            conversion_rate as conversion_rate,
            is_current as is_current,
            created_date as created_date,
            modified_date as modified_date,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_conversion_rates") }}

    )

select *
from source
