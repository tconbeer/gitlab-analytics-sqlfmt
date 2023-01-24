with
    source as (

        select
            id as creative_id,
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
            url_current as url_current,
            url_display as url_display,
            url_old as url_old,
            url_requested as url_requested,
            url_shortened as url_shortened,
            ad_type as ad_type,
            is_upgraded_url as is_upgraded_url,
            headline as headline,
            description_line_1 as description_line_1,
            description_line_2 as description_line_2,
            tracking_url_template as tracking_url_template,
            tracking_url_template_old as tracking_url_template_old,
            tracking_url_template_requested as tracking_url_template_requested,
            tracking_url_template_applied as tracking_url_template_applied,
            share_urn as share_urn,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_creatives") }}

    )

select *
from source
