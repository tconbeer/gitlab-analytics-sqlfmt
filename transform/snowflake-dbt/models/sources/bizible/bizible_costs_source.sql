with
    source as (

        select

            id as cost_id,
            modified_date as modified_date,
            cost_date as cost_date,
            source as source,
            cost_in_micro as cost_in_micro,
            clicks as clicks,
            impressions as impressions,
            estimated_total_possible_impressions
            as estimated_total_possible_impressions,
            ad_provider as ad_provider,
            channel_unique_id as channel_unique_id,
            channel_name as channel_name,
            channel_is_aggregatable_cost as channel_is_aggregatable_cost,
            advertiser_unique_id as advertiser_unique_id,
            advertiser_name as advertiser_name,
            advertiser_is_aggregatable_cost as advertiser_is_aggregatable_cost,
            account_unique_id as account_unique_id,
            account_name as account_name,
            account_is_aggregatable_cost as account_is_aggregatable_cost,
            campaign_unique_id as campaign_unique_id,
            campaign_name as campaign_name,
            campaign_is_aggregatable_cost as campaign_is_aggregatable_cost,
            ad_group_unique_id as ad_group_unique_id,
            ad_group_name as ad_group_name,
            ad_group_is_aggregatable_cost as ad_group_is_aggregatable_cost,
            ad_unique_id as ad_unique_id,
            ad_name as ad_name,
            ad_is_aggregatable_cost as ad_is_aggregatable_cost,
            creative_unique_id as creative_unique_id,
            creative_name as creative_name,
            creative_is_aggregatable_cost as creative_is_aggregatable_cost,
            keyword_unique_id as keyword_unique_id,
            keyword_name as keyword_name,
            keyword_is_aggregatable_cost as keyword_is_aggregatable_cost,
            placement_unique_id as placement_unique_id,
            placement_name as placement_name,
            placement_is_aggregatable_cost as placement_is_aggregatable_cost,
            site_unique_id as site_unique_id,
            site_name as site_name,
            site_is_aggregatable_cost as site_is_aggregatable_cost,
            is_deleted as is_deleted,
            iso_currency_code as iso_currency_code,
            source_id as source_id,
            row_key as row_key,
            account_row_key as account_row_key,
            advertiser_row_key as advertiser_row_key,
            site_row_key as site_row_key,
            placement_row_key as placement_row_key,
            campaign_row_key as campaign_row_key,
            ad_row_key as ad_row_key,
            ad_group_row_key as ad_group_row_key,
            creative_row_key as creative_row_key,
            keyword_row_key as keyword_row_key,
            currency_id as currency_id,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_costs") }}

    )

select *
from source
