with
    source as (

        select {{ hash_sensitive_columns("bizible_crm_events_source") }}
        from {{ ref("bizible_crm_events_source") }}

    )

select *
from source
