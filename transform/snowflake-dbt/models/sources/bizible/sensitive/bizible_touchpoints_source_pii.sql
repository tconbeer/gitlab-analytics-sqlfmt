with
    source as (

        select
            {{ nohash_sensitive_columns("bizible_touchpoints_source", "touchpoint_id") }}
        from {{ ref("bizible_touchpoints_source") }}

    )

select *
from source
