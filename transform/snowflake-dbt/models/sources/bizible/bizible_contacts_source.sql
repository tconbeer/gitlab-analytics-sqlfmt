with
    source as (

        select
            id as contact_id,
            modified_date as modified_date,
            created_date as created_date,
            email as email,
            accountid as accountid,
            lead_source as lead_source,
            bizible_stage as bizible_stage,
            bizible_stage_previous as bizible_stage_previous,
            odds_of_conversion as odds_of_conversion,
            bizible_cookie_id as bizible_cookie_id,
            is_deleted as is_deleted,
            is_duplicate as is_duplicate,
            source_system as source_system,
            other_system_id as other_system_id,
            custom_properties as custom_properties,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_contacts") }}

    )

select *
from source
