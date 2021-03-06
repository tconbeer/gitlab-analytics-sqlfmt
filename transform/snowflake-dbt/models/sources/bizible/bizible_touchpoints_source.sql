with
    source as (

        select
            id as touchpoint_id,
            modified_date as modified_date,
            email as email,
            contact_id as contact_id,
            account_id as account_id,
            lead_id as lead_id,
            unique_id_person as unique_id_person,
            user_touchpoint_id as user_touchpoint_id,
            visitor_id as visitor_id,
            touchpoint_date as touchpoint_date,
            marketing_touch_type as marketing_touch_type,
            channel as channel,
            category1 as category1,
            category2 as category2,
            category3 as category3,
            category4 as category4,
            category5 as category5,
            category6 as category6,
            category7 as category7,
            category8 as category8,
            category9 as category9,
            category10 as category10,
            category11 as category11,
            category12 as category12,
            category13 as category13,
            category14 as category14,
            category15 as category15,
            browser_name as browser_name,
            browser_version as browser_version,
            platform_name as platform_name,
            platform_version as platform_version,
            landing_page as landing_page,
            landing_page_raw as landing_page_raw,
            referrer_page as referrer_page,
            referrer_page_raw as referrer_page_raw,
            form_page as form_page,
            form_page_raw as form_page_raw,
            form_date as form_date,
            city as city,
            region as region,
            country as country,
            medium as medium,
            web_source as web_source,
            search_phrase as search_phrase,
            ad_provider as ad_provider,
            account_unique_id as account_unique_id,
            account_name as account_name,
            advertiser_unique_id as advertiser_unique_id,
            advertiser_name as advertiser_name,
            site_unique_id as site_unique_id,
            site_name as site_name,
            placement_unique_id as placement_unique_id,
            placement_name as placement_name,
            campaign_unique_id as campaign_unique_id,
            campaign_name as campaign_name,
            ad_group_unique_id as ad_group_unique_id,
            ad_group_name as ad_group_name,
            ad_unique_id as ad_unique_id,
            ad_name as ad_name,
            creative_unique_id as creative_unique_id,
            creative_name as creative_name,
            creative_description_1 as creative_description_1,
            creative_description_2 as creative_description_2,
            creative_destination_url as creative_destination_url,
            creative_display_url as creative_display_url,
            keyword_unique_id as keyword_unique_id,
            keyword_name as keyword_name,
            keyword_match_type as keyword_match_type,
            is_first_touch as is_first_touch,
            is_lead_creation_touch as is_lead_creation_touch,
            is_opp_creation_touch as is_opp_creation_touch,
            is_closed_touch as is_closed_touch,
            stages_touched as stages_touched,
            is_form_submission_touch as is_form_submission_touch,
            is_impression_touch as is_impression_touch,
            first_click_percentage as first_click_percentage,
            last_anon_click_percentage as last_anon_click_percentage,
            u_shape_percentage as u_shape_percentage,
            w_shape_percentage as w_shape_percentage,
            full_path_percentage as full_path_percentage,
            custom_model_percentage as custom_model_percentage,
            is_deleted as is_deleted,
            row_key as row_key,
            contact_row_key as contact_row_key,
            lead_row_key as lead_row_key,
            landing_page_key as landing_page_key,
            referrer_page_key as referrer_page_key,
            form_page_key as form_page_key,
            account_row_key as account_row_key,
            advertiser_row_key as advertiser_row_key,
            site_row_key as site_row_key,
            placement_row_key as placement_row_key,
            campaign_row_key as campaign_row_key,
            ad_row_key as ad_row_key,
            ad_group_row_key as ad_group_row_key,
            creative_row_key as creative_row_key,
            keyword_row_key as keyword_row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_touchpoints") }}

    )

select *
from source
