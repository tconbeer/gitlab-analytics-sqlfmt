with
    source as (select * from {{ source("snapshots", "sfdc_account_snapshots") }}),

    renamed as (

        select
            id as account_id,
            name as account_name,

            -- keys
            account_id_18__c as account_id_18,
            masterrecordid as master_record_id,
            ownerid as owner_id,
            parentid as parent_id,
            primary_contact_id__c as primary_contact_id,
            recordtypeid as record_type_id,
            ultimate_parent_account_id__c as ultimate_parent_id,
            partner_vat_tax_id__c as partner_vat_tax_id,

            -- key people GL side
            federal_account__c as federal_account,
            gitlab_com_user__c as gitlab_com_user,
            account_manager__c as account_manager,
            account_owner_calc__c as account_owner,
            account_owner_team__c as account_owner_team,
            business_development_rep__c as business_development_rep,
            dedicated_service_engineer__c as dedicated_service_engineer,
            sdr_assigned__c as sales_development_rep,
            -- solutions_architect__c                     AS solutions_architect,
            technical_account_manager_lu__c as technical_account_manager_id,

            -- info
            "{{ this.database }}".{{ target.schema }}.id15to18(
                substring(
                    regexp_replace(
                        ultimate_parent_account__c, '_HL_ENCODED_/|<a\\s+href="/', ''
                    ),
                    0,
                    15
                )
            ) as ultimate_parent_account_id,
            type as account_type,
            dfox_industry__c as df_industry,
            industry as industry,
            sub_industry__c as sub_industry,
            parent_lam_industry_acct_heirarchy__c as parent_account_industry_hierarchy,
            account_tier__c as account_tier,
            customer_since__c::date as customer_since_date,
            carr_this_account__c as carr_this_account,
            carr_acct_family__c as carr_account_family,
            next_renewal_date__c as next_renewal_date,
            license_utilization__c as license_utilization,
            support_level__c as support_level,
            named_account__c as named_account,
            billingcountry as billing_country,
            billingpostalcode as billing_postal_code,
            sdr_target_account__c::boolean as is_sdr_target_account,
            lam__c as lam,
            lam_dev_count__c as lam_dev_count,
            potential_arr_lam__c as potential_arr_lam,
            jihu_account__c::boolean as is_jihu_account,
            partners_signed_contract_date__c as partners_signed_contract_date,
            partner_account_iban_number__c as partner_account_iban_number,
            partners_partner_type__c as partner_type,
            partners_partner_status__c as partner_status,
            fy22_new_logo_target_list__c::boolean as fy22_new_logo_target_list,
            first_order_available__c::boolean as is_first_order_available,
            replace(
                zi_technologies__c,
                -- noqa:L016
                'The technologies that are used and not used at this account, according to ZoomInfo, after completing a scan are:',
                ''
            ) as zi_technologies,
            technical_account_manager_date__c::date as technical_account_manager_date,
            gitlab_customer_success_project__c::varchar
            as gitlab_customer_success_project,

            -- territory success planning fields
            atam_approved_next_owner__c as tsp_approved_next_owner,
            atam_next_owner_role__c as tsp_next_owner_role,
            atam_account_employees__c as tsp_account_employees,
            jb_max_family_employees__c as tsp_max_family_employees,
            trim(split_part(atam_region__c, '-', 1)) as tsp_region,
            trim(split_part(atam_sub_region__c, '-', 1)) as tsp_sub_region,
            trim(split_part(atam_area__c, '-', 1)) as tsp_area,
            atam_territory__c as tsp_territory,
            atam_address_country__c as tsp_address_country,
            atam_address_state__c as tsp_address_state,
            atam_address_city__c as tsp_address_city,
            atam_address_street__c as tsp_address_street,
            atam_address_postal_code__c as tsp_address_postal_code,

            -- account demographics fields
            account_demographics_sales_segment__c as account_demographics_sales_segment,
            account_demographics_geo__c as account_demographics_geo,
            account_demographics_region__c as account_demographics_region,
            account_demographics_area__c as account_demographics_area,
            account_demographics_territory__c as account_demographics_territory,
            account_demographics_employee_count__c
            as account_demographics_employee_count,
            account_demographic_max_family_employees__c
            as account_demographics_max_family_employee,
            account_demographics_upa_country__c as account_demographics_upa_country,
            account_demographics_upa_state__c as account_demographics_upa_state,
            account_demographics_upa_city__c as account_demographics_upa_city,
            account_demographics_upa_street__c as account_demographics_upa_street,
            account_demographics_upa_postal_code__c
            as account_demographics_upa_postal_code,

            -- present state info
            health__c as health_score,
            gs_health_score__c as health_number,
            gs_health_score_color__c as health_score_color,

            -- opportunity metrics
            count_of_active_subscription_charges__c
            as count_active_subscription_charges,
            count_of_active_subscriptions__c as count_active_subscriptions,
            count_of_billing_accounts__c as count_billing_accounts,
            license_user_count__c as count_licensed_users,
            count_of_new_business_won_opps__c
            as count_of_new_business_won_opportunities,
            count_of_open_renewal_opportunities__c as count_open_renewal_opportunities,
            count_of_opportunities__c as count_opportunities,
            count_of_products_purchased__c as count_products_purchased,
            count_of_won_opportunities__c as count_won_opportunities,
            concurrent_ee_subscriptions__c as count_concurrent_ee_subscriptions,
            ce_instances__c as count_ce_instances,
            active_ce_users__c as count_active_ce_users,
            number_of_open_opportunities__c as count_open_opportunities,
            using_ce__c as count_using_ce,

            -- account based marketing fields
            abm_tier__c as abm_tier,
            gtm_strategy__c as gtm_strategy,
            gtm_acceleration_date__c as gtm_acceleration_date,
            gtm_account_based_date__c as gtm_account_based_date,
            gtm_account_centric_date__c as gtm_account_centric_date,
            abm_tier_1_date__c as abm_tier_1_date,
            abm_tier_2_date__c as abm_tier_2_date,
            abm_tier_3_date__c as abm_tier_3_date,

            -- demandbase fields
            account_list__c as demandbase_account_list,
            intent__c as demandbase_intent,
            page_views__c as demandbase_page_views,
            score__c as demandbase_score,
            sessions__c as demandbase_sessions,
            trending_offsite_intent__c as demandbase_trending_offsite_intent,
            trending_onsite_engagement__c as demandbase_trending_onsite_engagement,

            -- sales segment fields
            ultimate_parent_sales_segment_employees__c as ultimate_parent_sales_segment,
            sales_segmentation_new__c as division_sales_segment,
            jb_test_sales_segment__c as tsp_max_hierarchy_sales_segment,
            account_owner_user_segment__c as account_owner_user_segment,
            -- ************************************
            -- sales segmentation deprecated fields - 2020-09-03
            -- left temporary for the sake of MVC and avoid breaking SiSense existing
            -- charts
            jb_test_sales_segment__c as tsp_test_sales_segment,
            ultimate_parent_sales_segment_employees__c as sales_segment,
            sales_segmentation_new__c as account_segment,

            -- ************************************
            -- NF: 2020-12-17
            -- these three fields are used to identify accounts owned by
            -- reps within hierarchies that they do not fully own
            -- or even within different regions
            locally_managed__c as is_locally_managed_account,
            strategic__c as is_strategic_account,

            -- ************************************
            -- New SFDC Account Fields for FY22 Planning
            next_fy_account_owner_temp__c as next_fy_account_owner_temp,
            next_fy_planning_notes_temp__c as next_fy_planning_notes_temp,

            -- *************************************
            -- Partner Account fields
            partner_track__c as partner_track,
            partners_partner_type__c as partners_partner_type,
            gitlab_partner_programs__c as gitlab_partner_program,

            -- *************************************
            -- Zoom Info Fields
            zi_account_name__c as zoom_info_company_name,
            zi_revenue__c as zoom_info_company_revenue,
            zi_employees__c as zoom_info_company_employee_count,
            zi_industry__c as zoom_info_company_industry,
            zi_city__c as zoom_info_company_city,
            zi_state_province__c as zoom_info_company_state_province,
            zi_country__c as zoom_info_company_country,
            exclude_from_zoominfo_enrich__c as is_excluded_from_zoom_info_enrich,
            zi_website__c as zoom_info_website,
            zi_company_other_domains__c as zoom_info_company_other_domains,
            dozisf__zoominfo_id__c as zoom_info_dozisf_zi_id,
            zi_parent_company_zoominfo_id__c as zoom_info_parent_company_zi_id,
            zi_parent_company_name__c as zoom_info_parent_company_name,
            zi_ultimate_parent_company_zoominfo_id__c
            as zoom_info_ultimate_parent_company_zi_id,
            zi_ultimate_parent_company_name__c
            as zoom_info_ultimate_parent_company_name,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            isdeleted as is_deleted,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            lastactivitydate as last_activity_date,
            convert_timezone(
                'America/Los_Angeles', convert_timezone('UTC', current_timestamp())
            ) as _last_dbt_run,
            systemmodstamp,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source
    )

select *
from renamed
