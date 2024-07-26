with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_opportunities_source", "opportunity_id"
                )
            }}
        from {{ ref("bizible_opportunities_source") }}

    )

select *
from source
