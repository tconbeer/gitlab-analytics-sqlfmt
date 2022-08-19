with
    source as (select * from {{ source("salesforce", "campaign") }}),
    renamed as (

        select
            id as campaign_id,
            name as campaign_name,
            isactive as is_active,
            startdate as start_date,
            enddate as end_date,
            status as status,
            iff(type like 'Field Event%', 'Field Event', type) as type,

            -- keys
            campaignmemberrecordtypeid as campaign_member_record_type_id,
            ownerid as campaign_owner_id,
            parentid as campaign_parent_id,

            -- info
            description as description,
            region__c as region,
            sub_region__c as sub_region,
            budget_holder__c as budget_holder,
            -- projections
            budgetedcost as budgeted_cost,
            expectedresponse as expected_response,
            expectedrevenue as expected_revenue,
            bizible2__bizible_attribution_synctype__c
            as bizible_touchpoint_enabled_setting,
            allocadia_id__c as allocadia_id,
            is_a_channel_partner_involved__c as is_a_channel_partner_involved,
            is_an_alliance_partner_involved__c as is_an_alliance_partner_involved,
            in_person_virtual__c as is_this_an_in_person_event,
            alliance_partner_name__c as alliance_partner_name,
            channel_partner_name__c as channel_partner_name,
            sales_play__c as sales_play,
            gtm_motion__c as gtm_motion,
            total_planned_mql__c as total_planned_mqls,

            -- results
            actualcost as actual_cost,
            amountallopportunities as amount_all_opportunities,
            amountwonopportunities as amount_won_opportunities,
            numberofcontacts as count_contacts,
            numberofconvertedleads as count_converted_leads,
            numberofleads as count_leads,
            numberofopportunities as count_opportunities,
            numberofresponses as count_responses,
            numberofwonopportunities as count_won_opportunities,
            numbersent as count_sent,
            strat_contribution__c as strategic_marketing_contribution,
            large_bucket__c as large_bucket,
            reporting_type__c as reporting_type,

            -- metadata
            createddate as created_date,
            createdbyid as created_by_id,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            lastactivitydate as last_activity_date,
            systemmodstamp,

            isdeleted as is_deleted

        from source
    )

select *
from renamed
