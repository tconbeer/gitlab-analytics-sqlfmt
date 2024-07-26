with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_attribution_touchpoints_source",
                    "attribution_touchpoint_id",
                )
            }}
        from {{ ref("bizible_attribution_touchpoints_source") }}

    )

select *
from source
