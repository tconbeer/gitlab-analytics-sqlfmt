with
    source as (

        select

            id as opp_stage_transition_id,
            account_id as account_id,
            opportunity_id as opportunity_id,
            contact_id as contact_id,
            email as email,
            touchpoint_id as touchpoint_id,
            transition_date as transition_date,
            stage_id as stage_id,
            stage as stage,
            rank as rank,
            index as index,
            last_index as last_index,
            is_pending as is_pending,
            is_non_transitional as is_non_transitional,
            previous_stage_date as previous_stage_date,
            next_stage_date as next_stage_date,
            modified_date as modified_date,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_opp_stage_transitions") }}

    )

select *
from source
