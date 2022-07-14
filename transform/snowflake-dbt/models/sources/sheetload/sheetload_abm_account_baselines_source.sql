with
    source as (select * from {{ source("sheetload", "abm_account_baselines") }}),
    renamed as (

        select
            "Date_Added"::date as added_date,
            "Account_ID"::varchar as account_id,
            "GTM_Strategy"::varchar as gtm_strategy,
            nullif("ABM_Tier", '')::varchar as abm_tier,
            nullif("Account_Name", '')::varchar as account_name,
            nullif("Parent_Account", '')::varchar as parent_account,
            nullif("Website", '')::varchar as website,
            nullif("Domains", '')::varchar as domains,
            nullif("Type", '')::varchar as type,
            nullif("Count_of_Contacts", '')::float as count_of_contacts,
            nullif("Health_Score", '')::varchar as health_score,
            nullif("Count_of_Opportunities", '')::float as count_of_opportunities,
            nullif("Number_of_Open_Opportunities", '')::float
            as number_of_open_opportunities,
            nullif("Count_of_Won_Opportunities", '')::float
            as count_of_won_opportunities,
            nullif("Total_Closed_Won_Amount_(All-Time)", '')::float
            as total_closed_won_amount,
            nullif("Sum:_Open_New_Add-on_IACV_Opportunities", '')::float
            as open_new_add_on_iacv,
            nullif("Sum_of_Open_Renewal_Opportunities", '')::float
            as sum_open_renewal_opportunities,
            nullif("Support_Level", '')::varchar as support_level,
            nullif("GitLab.com_user", '')::float as gitlab_com_user,
            nullif("GitLab_EE_Customer", '')::float as gitlab_ee_customer,
            nullif("EE_Basic_Customer", '')::float as ee_basic_customer,
            nullif("EE_Standard_Customer", '')::float as ee_standard_customer,
            nullif("EE_Plus_Customer", '')::float as ee_plus_customer,
            nullif("Concurrent_EE_Subscriptions", '')::float
            as concurrent_ee_subscriptions,
            nullif("Count_of_Active_Subscriptions", '')::float
            as count_active_subscriptions,
            nullif("Using_CE", '')::float as using_ce,
            nullif("CE_Instances", '')::float as ce_instances,
            nullif("Active_CE_Users", '')::float as active_ce_users,
            nullif("DemandBase:_Score", '')::varchar as demandbase_score,
            nullif("DemandBase:_Account_List", '')::varchar as demandbase_account_list,
            nullif("DemandBase:_Intent", '')::varchar as demandbase_intent,
            nullif("DemandBase:_Page_Views", '')::float as demandbase_page_views,
            nullif("DemandBase:_Sessions", '')::float as demandbase_sessions,
            nullif("DemandBase:_Trending_Onsite_Engagement", '')::float
            as demandbase_trending_onsite_engagement,
            nullif("DemandBase:_Trending_Offsite_Intent", '')::float
            as demandbase_trending_offsite_intent,
            nullif("Account_Owner", '')::varchar as account_owner,
            nullif("Billing_State_Province", '')::varchar as billing_state_province

        from source

    )

select *
from renamed
