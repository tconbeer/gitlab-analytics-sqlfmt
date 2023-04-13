with
    sfdc_campaign as (select * from {{ ref("sfdc_campaign") }}),
    xf as (

        select
            sfdc_campaign.campaign_id,
            sfdc_campaign.campaign_name,
            sfdc_campaign.description,
            sfdc_campaign.type as campaign_type,
            sfdc_campaign.start_date as campaign_start_date,
            sfdc_campaign.end_date as campaign_end_date,
            parent_campaign.campaign_name as parent_campaign_name,
            parent_campaign.type as parent_campaign_type,
            sfdc_campaign.is_active,
            sfdc_campaign.amount_all_opportunities,
            sfdc_campaign.amount_won_opportunities,
            sfdc_campaign.count_contacts,
            sfdc_campaign.count_converted_leads,
            sfdc_campaign.count_leads,
            sfdc_campaign.count_opportunities,
            sfdc_campaign.count_responses,
            sfdc_campaign.count_sent,
            sfdc_campaign.count_won_opportunities,
            sfdc_campaign.budget_holder,
            sfdc_campaign.strategic_marketing_contribution,
            sfdc_campaign.large_bucket,
            sfdc_campaign.reporting_type
        from sfdc_campaign
        left join
            sfdc_campaign as parent_campaign
            on sfdc_campaign.campaign_parent_id = parent_campaign.campaign_id

    )

select *
from xf
