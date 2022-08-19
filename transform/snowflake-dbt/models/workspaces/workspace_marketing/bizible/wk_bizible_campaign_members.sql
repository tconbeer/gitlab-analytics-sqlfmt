with
    source as (

        select {{ hash_sensitive_columns("bizible_campaign_members_source") }}
        from {{ ref("bizible_campaign_members_source") }}

    )

select *
from source
