with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_opp_stage_transitions_source", "opp_stage_transition_id"
                )
            }}
        from {{ ref("bizible_opp_stage_transitions_source") }}

    )

select *
from source
