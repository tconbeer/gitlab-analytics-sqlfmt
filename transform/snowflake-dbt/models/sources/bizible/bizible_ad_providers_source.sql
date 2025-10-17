with
    source as (

        select
            id as ad_provider_id,
            name as name,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_ad_providers") }}

    )

select *
from source
