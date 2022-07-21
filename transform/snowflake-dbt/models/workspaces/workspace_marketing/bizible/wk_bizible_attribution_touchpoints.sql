with
    source as (

        select {{ hash_sensitive_columns("bizible_attribution_touchpoints_source") }}
        from {{ ref("bizible_attribution_touchpoints_source") }}

    )

select *
from source
