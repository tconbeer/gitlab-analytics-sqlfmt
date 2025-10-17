with
    source as (

        select {{ hash_sensitive_columns("bizible_lead_stage_transitions_source") }}
        from {{ ref("bizible_lead_stage_transitions_source") }}

    )

select *
from source
