with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_lead_stage_transitions_source", "lead_stage_transition_id"
                )
            }}
        from {{ ref("bizible_lead_stage_transitions_source") }}

    )

select *
from source
