with
    source as (

        select
            id as url_id,
            scheme as scheme,
            host as host,
            port as port,
            path as path,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_urls") }}

    )

select *
from source
