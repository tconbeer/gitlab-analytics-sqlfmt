with
    source as (

        select

            id as campaign_member_id,
            modified_date as modified_date,
            created_date as created_date,
            bizible_touch_point_date as bizible_touch_point_date,
            lead_id as lead_id,
            lead_email as lead_email,
            contact_id as contact_id,
            contact_email as contact_email,
            status as status,
            has_responded as has_responded,
            first_responded_date as first_responded_date,
            campaign_name as campaign_name,
            campaign_id as campaign_id,
            campaign_type as campaign_type,
            campaign_sync_type as campaign_sync_type,
            lead_sync_status as lead_sync_status,
            contact_sync_status as contact_sync_status,
            opp_sync_status as opp_sync_status,
            is_deleted as is_deleted,
            custom_properties as custom_properties,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_campaign_members") }}

    )

select *
from source
