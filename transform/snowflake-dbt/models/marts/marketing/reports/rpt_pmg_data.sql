{{ config({"schema": "common_mart_marketing"}) }}

{{ simple_cte([("pmg_paid_digital", "pmg_paid_digital")]) }},
final as (

    select
        *,
        date_trunc('month', reporting_date)::date::date as reporting_date_month_yr,
        reporting_date::date as reporting_date_normalized,
        case
            when campaign like '%_apac_%'
            then 'APAC'
            when campaign like '%_APAC_%'
            then 'APAC'
            when campaign like '%_emea_%'
            then 'EMEA'
            when campaign like '%_EMEA_%'
            then 'EMEA'
            when campaign like '%_amer_%'
            then 'AMER'
            when campaign like '%_AMER_%'
            then 'AMER'
            when campaign like '%_sanfrancisco_%'
            then 'AMER'
            when campaign like '%_seattle_%'
            then 'AMER'
            when campaign like '%_lam_%'
            then 'LATAM'
            when campaign like '%_sam_%'
            then 'LATAM'
            else 'Global'
        end as region_normalized,
        case
            when medium = 'cpc'
            then 'Paid Search'
            when medium = 'display'
            then 'Display'
            when medium = 'paidsocial'
            then 'Paid Social'
            else 'Other'
        end as mapped_channel,
        case
            when source = 'google'
            then 'Google'
            when source = 'bing_yahoo'
            then 'Bing'
            when source = 'facebook'
            then 'Facebook'
            when source = 'linkedin'
            then 'LinkedIn'
            when source = 'twitter'
            then 'Twitter'
            else 'Other'
        end as mapped_source,
        concat((mapped_channel), '.', (mapped_source)) as mapped_channel_source,
        case
            when mapped_channel_source = 'Other.Other'
            then 'Other'
            when mapped_channel_source = 'Paid Search.Google'
            then 'Paid Search.AdWords'
            else mapped_channel_source
        end as mapped_channel_source_normalized,
        case
            when content_type = 'Free Trial'
            then 'Free Trial'
            when team = 'digital' and medium = 'sponsorship'
            then 'Publishers/Sponsorships'  -- team=digital  captures the paid ads for digital and not including field 
            when campaign_code like '%operationalefficiencies%'
            then 'Increase Operational Efficiencies'
            when campaign_code like '%operationalefficiences%'
            then 'Increase Operational Efficiencies'
            when campaign_code like '%betterproductsfaster%'
            then 'Deliver Better Products Faster'
            when campaign_code like '%reducesecurityrisk%'
            then 'Reduce Security and Compliance Risk'
            when campaign_code like '%cicdcmp2%'
            then 'Jenkins Take Out'
            when campaign_code like '%cicdcmp3%'
            then 'CI Build & Test Auto'
            when campaign_code like '%octocat%'
            then 'OctoCat'
            when campaign_code like '%21q4-jp%'
            then 'Japan-Digital Readiness'
            when (campaign_code like '%singleappci%' and campaign like '%france%')
            then 'CI Use Case - FR'
            when (campaign_code like '%singleappci%' and campaign like '%germany%')
            then 'CI Use Case - DE'
            when campaign_code like '%singleappci%'
            then 'CI Use Case'
            when campaign_code like '%devsecopsusecase%'
            then 'DevSecOps Use Case'
            when campaign_code like '%awspartner%'
            then 'AWS'
            when campaign_code like '%vccusecase%'
            then 'VCC Use Case'
            when campaign_code like '%iacgitops%'
            then 'GitOps Use Case'
            when campaign_code like '%evergreen%'
            then 'Evergreen'
            when campaign_code like 'brand%'
            then 'Brand'
            when campaign_code like 'Brand%'
            then 'Brand'
            when campaign_code like '%simplifydevops%'
            then 'Simplify DevOps'
            when campaign_code like '%premtoultimatesp%'
            then 'Premium to Ultimate'
            when campaign_code like '%devopsgtm%'
            then 'DevOps GTM'
            when campaign_code like '%gitlab14%'
            then 'GitLab 14 webcast'
            when campaign_code like '%devopsgtm%' and content like '%partnercredit%'
            then 'Cloud Partner  Campaign'
            when campaign_code like '%devopsgtm%' and content like '%introtomlopsdemo%'
            then 'Technical  Demo Series'
            when
                campaign_code like '%psdigitaltransformation%'
                or campaign_code like '%psglobal%'
            then 'PubSec Nurture'
            else 'None'
        end as integrated_campaign_grouping,
        case
            when budget like '%x-ent%'
            then 'Large'
            when budget like '%x-mm%'
            then 'Mid-Market'
            when budget like '%x-smb%'
            then 'SMB'
            when budget like '%x-pr%'
            then 'Prospecting'
            when budget like '%x-rtg%'
            then 'Retargeting'
            else null
        end as utm_segment,
        iff(
            integrated_campaign_grouping <> 'None', 'Demand Gen', 'Other'
        ) as touchpoint_segment,
        case
            when
                integrated_campaign_grouping in (
                    'CI Build & Test Auto',
                    'CI Use Case',
                    'CI Use Case - FR',
                    'CI Use Case - DE',
                    'CI/CD Seeing is Believing',
                    'Jenkins Take Out',
                    'OctoCat',
                    'Premium to Ultimate'
                )
            then 'CI/CD'
            when
                integrated_campaign_grouping in (
                    'Deliver Better Products Faster',
                    'DevSecOps Use Case',
                    'Reduce Security and Compliance Risk',
                    'Simplify DevOps',
                    'DevOps GTM',
                    'Cloud Partner Campaign',
                    'GitLab 14 webcast',
                    'Technical Demo Series'
                )
            then 'DevOps'
            when integrated_campaign_grouping = 'GitOps Use Case'
            then 'GitOps'
            else null
        end as gtm_motion
    from pmg_paid_digital

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@rkohnke",
        updated_by="@rkohnke",
        created_date="2022-01-25",
        updated_date="2022-02-02",
    )
}}
