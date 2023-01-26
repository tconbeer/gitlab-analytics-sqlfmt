with
    source as (

        select
            {{ nohash_sensitive_columns("bizible_crm_events_source", "crm_event_id") }}
        from {{ ref("bizible_crm_events_source") }}

    )

select *
from source
