with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_user_touchpoints_source", "user_touchpoint_id"
                )
            }}
        from {{ ref("bizible_user_touchpoints_source") }}

    )

select *
from source
