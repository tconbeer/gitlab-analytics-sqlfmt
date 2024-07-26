with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_campaign_members_source", "campaign_member_id"
                )
            }}
        from {{ ref("bizible_campaign_members_source") }}

    )

select *
from source
