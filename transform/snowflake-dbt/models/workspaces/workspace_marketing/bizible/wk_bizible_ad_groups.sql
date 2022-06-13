with
    source as (

        select {{ hash_sensitive_columns("bizible_ad_groups_source") }}
        from {{ ref("bizible_ad_groups_source") }}

    )

select *
from source
