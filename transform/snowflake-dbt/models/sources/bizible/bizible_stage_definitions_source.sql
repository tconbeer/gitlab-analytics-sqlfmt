with
    source as (

        select
            id as stage_definition_id,
            modified_date as modified_date,
            stage_name as stage_name,
            is_inactive as is_inactive,
            is_in_custom_model as is_in_custom_model,
            is_boomerang as is_boomerang,
            is_transition_tracking as is_transition_tracking,
            stage_status as stage_status,
            is_from_salesforce as is_from_salesforce,
            is_default as is_default,
            rank as rank,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_stage_definitions") }}

    )

select *
from source
