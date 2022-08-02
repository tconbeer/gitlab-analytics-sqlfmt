with
    source as (

        select {{ hash_sensitive_columns("bizible_touchpoints_source") }}
        from {{ ref("bizible_touchpoints_source") }}

    )

select *
from source
