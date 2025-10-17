with
    source as (

        select {{ hash_sensitive_columns("bizible_urls_source") }}
        from {{ ref("bizible_urls_source") }}

    )

select *
from source
