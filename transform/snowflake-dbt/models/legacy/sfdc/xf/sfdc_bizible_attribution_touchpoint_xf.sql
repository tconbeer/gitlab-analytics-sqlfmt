{{ config(tags=["mnpi_exception"]) }}

with
    opps as (select * from {{ ref("sfdc_opportunity_xf") }}),
    touches as (select * from {{ ref("sfdc_bizible_attribution_touchpoint") }}),
    final as (

        select
            date_trunc('month', opps.sales_accepted_date) as sales_accepted_month,
            date_trunc('month', opps.sales_qualified_date) as sales_qualified_month,
            date_trunc('month', opps.close_date) as close_month,
            touches.*,
            case
                -- Specific touchpoint overrides
                when touchpoint_id ilike 'a6061000000CeS0%'
                then 'Field Event'
                when bizible_marketing_channel_path = 'CPC.AdWords'
                then 'Google AdWords'
                when
                    bizible_marketing_channel_path in (
                        'Email.Other', 'Email.Newsletter', 'Email.Outreach'
                    )
                then 'Email'
                when
                    bizible_marketing_channel_path in (
                        'Field Event',
                        'Partners.Google',
                        'Brand.Corporate Event',
                        'Conference',
                        'Speaking Session'
                    )
                    or (
                        bizible_medium = 'Field Event (old)'
                        and bizible_marketing_channel_path = 'Other'
                    )
                then 'Field Event'
                when
                    bizible_marketing_channel_path in (
                        'Paid Social.Facebook',
                        'Paid Social.LinkedIn',
                        'Paid Social.Twitter',
                        'Paid Social.YouTube'
                    )
                then 'Paid Social'
                when
                    bizible_marketing_channel_path in (
                        'Social.Facebook',
                        'Social.LinkedIn',
                        'Social.Twitter',
                        'Social.YouTube'
                    )
                then 'Social'
                when
                    bizible_marketing_channel_path in (
                        'Marketing Site.Web Referral', 'Web Referral'
                    )
                then 'Web Referral'
                when
                    bizible_marketing_channel_path in (
                        'Marketing Site.Web Direct', 'Web Direct'
                    )
                    -- Added to Web Direct
                    or campaign_id in (
                        '701610000008ciRAAQ',  -- Trial - GitLab.com
                        '70161000000VwZbAAK',  -- Trial - Self-Managed
                        '70161000000VwZgAAK',  -- Trial - SaaS
                        '70161000000CnSLAA0',  -- 20181218_DevOpsVirtual
                        '701610000008cDYAAY'  -- 2018_MovingToGitLab
                    )
                then 'Web Direct'
                when
                    bizible_marketing_channel_path like 'Organic Search.%'
                    or bizible_marketing_channel_path = 'Marketing Site.Organic'
                then 'Organic Search'
                when bizible_marketing_channel_path in ('Sponsorship')
                then 'Paid Sponsorship'
                else 'Unknown'
            end as pipe_name,
            opps.incremental_acv
            * touches.bizible_attribution_percent_full_path as iacv_full_path,
            opps.sales_type,
            opps.lead_source,
            opps.record_type_label
        from opps
        inner join touches on touches.opportunity_id = opps.opportunity_id

    )

select *
from final
