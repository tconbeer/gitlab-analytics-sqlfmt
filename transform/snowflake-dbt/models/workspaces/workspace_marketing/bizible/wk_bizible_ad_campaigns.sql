with
    source as (

        select {{ hash_sensitive_columns("bizible_ad_campaigns_source") }}
        from {{ ref("bizible_ad_campaigns_source") }}

    )

select *
from source
