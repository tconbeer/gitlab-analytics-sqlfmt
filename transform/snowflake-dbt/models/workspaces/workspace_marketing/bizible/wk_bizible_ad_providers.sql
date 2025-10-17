with
    source as (

        select {{ hash_sensitive_columns("bizible_ad_providers_source") }}
        from {{ ref("bizible_ad_providers_source") }}

    )

select *
from source
