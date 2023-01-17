with
    source as (

        select
            id as site_link_id,
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
            url_old as url_old,
            url_requested as url_requested,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_site_links") }}

    )

select *
from source
