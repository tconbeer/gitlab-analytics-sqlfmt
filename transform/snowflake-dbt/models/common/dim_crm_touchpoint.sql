with
    campaign_details as (select * from {{ ref("prep_campaign") }}),
    bizible_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    bizible_attribution_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_attribution_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    bizible_touchpoints_with_campaign as (

        select
            bizible_touchpoints.*,
            campaign_details.dim_campaign_id,
            campaign_details.dim_parent_campaign_id
        from bizible_touchpoints
        left join
            campaign_details
            on bizible_touchpoints.campaign_id = campaign_details.dim_campaign_id

    ),
    bizible_attribution_touchpoints_with_campaign as (

        select
            bizible_attribution_touchpoints.*,
            campaign_details.dim_campaign_id,
            campaign_details.dim_parent_campaign_id
        from bizible_attribution_touchpoints
        left join
            campaign_details
            on bizible_attribution_touchpoints.campaign_id
            = campaign_details.dim_campaign_id

    ),
    bizible_campaign_grouping as (

        select * from {{ ref("map_bizible_campaign_grouping") }}

    ),
    combined_touchpoints as (

        select
            -- ids
            touchpoint_id as dim_crm_touchpoint_id,
            -- touchpoint info
            bizible_touchpoint_date,
            bizible_touchpoint_position,
            bizible_touchpoint_source,
            bizible_touchpoint_source_type,
            bizible_touchpoint_type,
            bizible_ad_campaign_name,
            bizible_ad_content,
            bizible_ad_group_name,
            bizible_form_url,
            bizible_form_url_raw,
            bizible_landing_page,
            bizible_landing_page_raw,
            bizible_marketing_channel,
            bizible_marketing_channel_path,
            bizible_medium,
            bizible_referrer_page,
            bizible_referrer_page_raw,
            bizible_salesforce_campaign,
            utm_content,
            '0' as is_attribution_touchpoint,
            dim_campaign_id,
            dim_parent_campaign_id

        from bizible_touchpoints_with_campaign

        UNION ALL

        select
            -- ids
            touchpoint_id as dim_crm_touchpoint_id,
            -- touchpoint info
            bizible_touchpoint_date,
            bizible_touchpoint_position,
            bizible_touchpoint_source,
            bizible_touchpoint_source_type,
            bizible_touchpoint_type,
            bizible_ad_campaign_name,
            bizible_ad_content,
            bizible_ad_group_name,
            bizible_form_url,
            bizible_form_url_raw,
            bizible_landing_page,
            bizible_landing_page_raw,
            bizible_marketing_channel,
            bizible_marketing_channel_path,
            bizible_medium,
            bizible_referrer_page,
            bizible_referrer_page_raw,
            bizible_salesforce_campaign,
            utm_content,
            '1' as is_attribution_touchpoint,
            dim_campaign_id,
            dim_parent_campaign_id

        from bizible_attribution_touchpoints_with_campaign

    ),
    final as (

        select
            combined_touchpoints.dim_crm_touchpoint_id,
            combined_touchpoints.bizible_touchpoint_date,
            combined_touchpoints.bizible_touchpoint_position,
            combined_touchpoints.bizible_touchpoint_source,
            combined_touchpoints.bizible_touchpoint_source_type,
            combined_touchpoints.bizible_touchpoint_type,
            combined_touchpoints.bizible_ad_campaign_name,
            combined_touchpoints.bizible_ad_content,
            combined_touchpoints.bizible_ad_group_name,
            combined_touchpoints.bizible_form_url,
            combined_touchpoints.bizible_form_url_raw,
            combined_touchpoints.bizible_landing_page,
            combined_touchpoints.bizible_landing_page_raw,
            combined_touchpoints.bizible_marketing_channel,
            combined_touchpoints.bizible_marketing_channel_path,
            combined_touchpoints.bizible_medium,
            combined_touchpoints.bizible_referrer_page,
            combined_touchpoints.bizible_referrer_page_raw,
            combined_touchpoints.bizible_salesforce_campaign,
            combined_touchpoints.utm_content,
            combined_touchpoints.is_attribution_touchpoint,
            bizible_campaign_grouping.integrated_campaign_grouping,
            bizible_campaign_grouping.bizible_integrated_campaign_grouping,
            bizible_campaign_grouping.gtm_motion,
            bizible_campaign_grouping.touchpoint_segment,
            case
                -- Specific touchpoint overrides
                when combined_touchpoints.dim_crm_touchpoint_id ilike 'a6061000000CeS0%'
                then 'Field Event'
                when combined_touchpoints.bizible_marketing_channel_path = 'CPC.AdWords'
                then 'Google AdWords'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Email.Other', 'Email.Newsletter', 'Email.Outreach'
                    )
                then 'Email'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Field Event',
                        'Partners.Google',
                        'Brand.Corporate Event',
                        'Conference',
                        'Speaking Session'
                    ) or (
                        combined_touchpoints.bizible_medium = 'Field Event (old)'
                        and combined_touchpoints.bizible_marketing_channel_path
                        = 'Other'
                    )
                then 'Field Event'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Paid Social.Facebook',
                        'Paid Social.LinkedIn',
                        'Paid Social.Twitter',
                        'Paid Social.YouTube'
                    )
                then 'Paid Social'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Social.Facebook',
                        'Social.LinkedIn',
                        'Social.Twitter',
                        'Social.YouTube'
                    )
                then 'Social'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Marketing Site.Web Referral', 'Web Referral'
                    )
                then 'Web Referral'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Marketing Site.Web Direct', 'Web Direct'
                    -- Added to Web Direct
                    ) or combined_touchpoints.dim_campaign_id in (
                        '701610000008ciRAAQ',  -- Trial - GitLab.com
                        '70161000000VwZbAAK',  -- Trial - Self-Managed
                        '70161000000VwZgAAK',  -- Trial - SaaS
                        '70161000000CnSLAA0',  -- 20181218_DevOpsVirtual
                        '701610000008cDYAAY'  -- 2018_MovingToGitLab
                    )
                then 'Web Direct'
                when
                    combined_touchpoints.bizible_marketing_channel_path
                    like 'Organic Search.%'
                    or combined_touchpoints.bizible_marketing_channel_path
                    = 'Marketing Site.Organic'
                then 'Organic Search'
                when
                    combined_touchpoints.bizible_marketing_channel_path in (
                        'Sponsorship'
                    )
                then 'Paid Sponsorship'
                else 'Unknown'
            end as pipe_name
        from combined_touchpoints
        left join
            bizible_campaign_grouping
            on combined_touchpoints.dim_crm_touchpoint_id
            = bizible_campaign_grouping.dim_crm_touchpoint_id
    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@rkohnke",
            created_date="2021-01-21",
            updated_date="2021-12-16",
        )
    }}
