with
    sfdc_account_snapshots as (select * from {{ ref("sfdc_account_snapshots_base") }}),
    final as (

        select
            -- keys
            date_actual,
            valid_from,
            valid_to,
            is_currently_valid,
            account_snapshot_id,
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
            ultimate_parent_sales_segment_employees__c as sales_segment,
            sales_segmentation_new__c as account_segment,
            "{{this.database}}".{{ target.schema }}.id15to18(
                substring(
                    regexp_replace(
                        ultimate_parent_account__c, '_HL_ENCODED_/|<a\\s+href="/', ''
                    ),
                    0,
                    15
                )
            ) as ultimate_parent_account_id,
            type as account_type,
            industry as industry,
            account_tier__c as account_tier,
            customer_since__c::date as customer_since_date,
            carr_acct_family__c as carr_account_family,
            next_renewal_date__c as next_renewal_date,
            license_utilization__c as license_utilization,
            products_purchased__c as products_purchased,
            support_level__c as support_level,
            named_account__c as named_account,
            billingcountry as billing_country,
            billingpostalcode as billing_postal_code,
            sdr_target_account__c::boolean as is_sdr_target_account,

            -- territory success planning fields
            atam_approved_next_owner__c as tsp_approved_next_owner,
            atam_next_owner_role__c as tsp_next_owner_role,
            jb_max_family_employees__c as tsp_max_family_employees,
            jb_test_sales_segment__c as tsp_test_sales_segment,
            atam_region__c as tsp_region,
            atam_sub_region__c as tsp_sub_region,
            atam_area__c as tsp_area,
            atam_territory__c as tsp_territory,
            atam_address_country__c as tsp_address_country,
            atam_address_state__c as tsp_address_state,
            atam_address_city__c as tsp_address_city,
            atam_address_street__c as tsp_address_street,
            atam_address_postal_code__c as tsp_address_postal_code,

            -- present state info
            health__c as health_score,

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
            systemmodstamp

        from sfdc_account_snapshots
        where id is not null and isdeleted = false

    )

select *
from final
