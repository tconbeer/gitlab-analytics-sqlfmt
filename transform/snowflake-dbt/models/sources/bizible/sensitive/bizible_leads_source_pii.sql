with
    source as (

        select {{ nohash_sensitive_columns("bizible_leads_source", "lead_id") }}
        from {{ ref("bizible_leads_source") }}

    )

select *
from source
