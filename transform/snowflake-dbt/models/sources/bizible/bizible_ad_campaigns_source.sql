with
    source as (

        select
            id as ad_campaign_id,
            display_id as display_id,
            ad_account_unique_id as ad_account_unique_id,
            ad_account_name as ad_account_name,
            advertiser_unique_id as advertiser_unique_id,
            advertiser_name as advertiser_name,
            ad_group_unique_id as ad_group_unique_id,
            ad_group_name as ad_group_name,
            ad_campaign_unique_id as ad_campaign_unique_id,
            ad_campaign_name as ad_campaign_name,
            is_active as is_active,
            is_deleted as is_deleted,
            modified_date as modified_date,
            first_imported as first_imported,
            name as name,
            needs_update as needs_update,
            grouping_key as grouping_key,
            entity_type as entity_type,
            provider_type as provider_type,
            daily_budget as daily_budget,
            tracking_url_template as tracking_url_template,
            tracking_url_template_old as tracking_url_template_old,
            tracking_url_template_requested as tracking_url_template_requested,
            tracking_url_template_applied as tracking_url_template_applied,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_ad_campaigns") }}

    )

select *
from source
