with
    source as (

        select
            id as crm_event_id,
            created_date as created_date,
            modified_date as modified_date,
            lead_id as lead_id,
            lead_email as lead_email,
            contact_id as contact_id,
            contact_email as contact_email,
            bizible_cookie_id as bizible_cookie_id,
            activity_type as activity_type,
            event_start_date as event_start_date,
            event_end_date as event_end_date,
            is_deleted as is_deleted,
            custom_properties as custom_properties,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_crm_events") }}

    )

select *
from source
