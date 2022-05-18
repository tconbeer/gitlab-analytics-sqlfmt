with
    source as (

        select {{ hash_sensitive_columns("bizible_ads_source") }}
        from {{ ref("bizible_ads_source") }}

    )

select *
from source
