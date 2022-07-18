with
    source as (

        select
            id as activities_id,
            lead_id as lead_id,
            contact_id as contact_id,
            activity_type_id as activity_type_id,
            activity_type_name as activity_type_name,
            start_date as start_date,
            end_date as end_date,
            campaign_id as campaign_id,
            source_system as source_system,
            created_date as created_date,
            modified_date as modified_date,
            is_deleted as is_deleted,
            ad_form_id as ad_form_id,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_activities") }}

    )

select *
from source
