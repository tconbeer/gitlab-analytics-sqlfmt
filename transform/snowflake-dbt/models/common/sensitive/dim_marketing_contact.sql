with
    sfdc_lead as (select * from {{ ref("sfdc_lead_source") }}),
    sfdc_contact as (select * from {{ ref("sfdc_contact_source") }}),
    sfdc_account as (select * from {{ ref("sfdc_account_source") }}),
    marketo as (select * from {{ ref("marketo_lead_source") }}),
    crm_account as (select * from {{ ref("dim_crm_account") }}),
    sales_segment as (select * from {{ ref("prep_sales_segment") }}),
    crm_person as (select * from {{ ref("prep_crm_person") }}),
    gitlab_users as (select * from {{ ref("gitlab_dotcom_users_source") }}),
    customer_db_source as (select * from {{ ref("customers_db_customers_source") }}),
    zuora_contact_source as (select * from {{ ref("zuora_contact_source") }}),
    zuora_account_source as (select * from {{ ref("zuora_account_source") }}),
    dnc_list as (

        select
            *,
            row_number() over (
                partition by email_address
                order by
                    case
                        when result in ('undeliverable', 'do_not_send') then 2 else 1
                    end desc
            ) as record_number
        from {{ ref("dnc_list") }}
        qualify record_number = 1

    ),
    sfdc as (

        select
            crm_person.sfdc_record_id,
            crm_person.dim_crm_account_id,
            case
                when crm_person.sfdc_record_type = 'contact'
                then sfdc_contact.contact_email
                else sfdc_lead.lead_email
            end as email_address,
            crm_person.dim_crm_person_id as crm_person_id,
            crm_person.sfdc_record_type as sfdc_lead_contact,
            case
                when sfdc_lead_contact = 'contact'
                then sfdc_contact.contact_first_name
                else sfdc_lead.lead_first_name
            end as first_name,
            case
                when
                    sfdc_lead_contact = 'contact'
                    and sfdc_contact.contact_last_name = '[[unknown]]'
                then null
                when
                    sfdc_lead_contact = 'contact'
                    and sfdc_contact.contact_last_name <> '[[unknown]]'
                then sfdc_contact.contact_last_name
                when
                    sfdc_lead_contact = 'lead'
                    and sfdc_lead.lead_last_name = '[[unknown]]'
                then null
                when
                    sfdc_lead_contact = 'lead'
                    and sfdc_lead.lead_last_name <> '[[unknown]]'
                then sfdc_lead.lead_last_name
            end as last_name,
            case
                when
                    sfdc_lead_contact = 'contact'
                    and sfdc_account.account_name = '[[unknown]]'
                then null
                when
                    sfdc_lead_contact = 'contact'
                    and sfdc_account.account_name <> '[[unknown]]'
                then sfdc_account.account_name
                when sfdc_lead_contact = 'lead' and sfdc_lead.company = '[[unknown]]'
                then null
                when sfdc_lead_contact = 'lead' and sfdc_lead.company <> '[[unknown]]'
                then sfdc_lead.company
            end as company_name,
            crm_person.title as job_title,
            crm_person.it_job_title_hierarchy,
            crm_account.parent_crm_account_sales_segment,
            crm_account.parent_crm_account_tsp_region,
            sfdc_account.tsp_region,
            crm_person.account_demographics_geo as crm_person_region,
            case
                when sfdc_lead_contact = 'contact'
                then sfdc_contact.mailing_country
                else sfdc_lead.country
            end as country,
            sfdc_contact.mobile_phone,
            case
                when sfdc_lead_contact = 'contact'
                then sfdc_contact.created_date
                else sfdc_lead.created_date
            end as sfdc_created_date,
            crm_person.has_opted_out_email as opted_out_salesforce,
            (
                row_number() over (
                    partition by email_address order by sfdc_created_date desc
                )
            ) as record_number

        from crm_person
        left join sfdc_contact on sfdc_contact.contact_id = crm_person.sfdc_record_id
        left join sfdc_lead on sfdc_lead.lead_id = sfdc_record_id
        left join sfdc_account on sfdc_account.account_id = sfdc_contact.account_id
        left join
            crm_account
            on crm_account.dim_crm_account_id = crm_person.dim_crm_account_id
        where email_address is not null and email_address <> ''
        qualify record_number = 1

    ),
    marketo_lead as (

        select
            marketo_lead_id,
            email as email_address,
            first_name,
            last_name,
            iff(company_name = '[[unknown]]', null, company_name) as company_name,
            job_title,
            it_job_title_hierarchy,
            country,
            mobile_phone,
            is_lead_inactive,
            is_contact_inactive,
            iff(
                sales_segmentation = 'Unknown', null, sales_segmentation
            ) as sales_segmentation,
            is_email_bounced as is_marketo_email_bounced,
            email_bounced_date as marketo_email_bounced_date,
            is_unsubscribed as is_marketo_unsubscribed,
            compliance_segment_value as marketo_compliance_segment_value,
            (
                row_number() over (partition by email order by updated_at desc)
            ) as record_number

        from marketo
        where email is not null or email <> ''
        qualify record_number = 1

    ),
    gitlab_dotcom as (

        select
            coalesce(notification_email, email) as email_address,
            user_id as user_id,
            split_part(users_name, ' ', 1) as first_name,
            split_part(users_name, ' ', 2) as last_name,
            user_name as user_name,
            organization as company_name,
            role as job_title,
            it_job_title_hierarchy,
            created_at as created_date,
            confirmed_at as confirmed_date,
            state as active_state,
            last_sign_in_at as last_login_date,
            is_email_opted_in as email_opted_in,
            (
                row_number() over (
                    partition by email_address order by created_date desc
                )
            ) as record_number
        from gitlab_users
        where
            email_address is not null
            and email_address <> ''
            and active_state = 'active'
        qualify record_number = 1

    ),
    customer_db as (

        select
            customer_email as email_address,
            customer_id as customer_id,
            customer_first_name as first_name,
            customer_last_name as last_name,
            company as company_name,
            country as country,
            customer_created_at as created_date,
            confirmed_at as confirmed_date,
            company_size as market_segment,
            last_sign_in_at as last_login_date,
            (
                row_number() over (
                    partition by email_address order by created_date desc
                )
            ) as record_number
        from customer_db_source
        where
            email_address is not null
            and email_address <> ''
            and confirmed_at is not null
        qualify record_number = 1

    ),
    zuora as (

        select
            zuora_contact_source.work_email as email_address,
            zuora_contact_source.contact_id as contact_id,
            zuora_contact_source.first_name as first_name,
            zuora_contact_source.last_name as last_name,
            zuora_account_source.account_name as company_name,
            zuora_contact_source.country as country,
            zuora_contact_source.created_date as created_date,
            case
                when zuora_contact_source.is_deleted = true
                then 'Inactive'
                else 'Active'
            end as active_state,
            (
                row_number() over (
                    partition by email_address
                    order by zuora_contact_source.created_date desc
                )
            ) as record_number
        from zuora_contact_source
        inner join
            zuora_account_source
            on zuora_account_source.account_id = zuora_contact_source.account_id
        where
            email_address is not null
            and email_address <> ''
            and zuora_contact_source.is_deleted = false
        qualify record_number = 1

    ),
    emails as (

        select email_address
        from sfdc

        union

        select email_address
        from gitlab_dotcom

        union

        select email_address
        from customer_db

        union

        select email_address
        from zuora

        union

        select email_address
        from marketo_lead

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["emails.email_address"]) }}
            as dim_marketing_contact_id,
            emails.email_address,
            coalesce(
                zuora.first_name,
                marketo_lead.first_name,
                sfdc.first_name,
                customer_db.first_name,
                gitlab_dotcom.first_name
            ) as first_name,
            coalesce(
                zuora.last_name,
                marketo_lead.last_name,
                sfdc.last_name,
                customer_db.last_name,
                gitlab_dotcom.last_name
            ) as last_name,
            gitlab_dotcom.user_name as gitlab_user_name,
            coalesce(
                zuora.company_name,
                marketo_lead.company_name,
                sfdc.company_name,
                customer_db.company_name,
                gitlab_dotcom.company_name
            ) as company_name,
            coalesce(
                marketo_lead.job_title, sfdc.job_title, gitlab_dotcom.job_title
            ) as job_title,
            case
                when marketo_lead.job_title is not null
                then marketo_lead.it_job_title_hierarchy
                when sfdc.job_title is not null
                then sfdc.it_job_title_hierarchy
                else gitlab_dotcom.it_job_title_hierarchy
            end as it_job_title_hierarchy,
            coalesce(
                zuora.country, marketo_lead.country, sfdc.country, customer_db.country
            ) as country,
            sfdc.parent_crm_account_sales_segment as sfdc_parent_sales_segment,
            coalesce(
                sfdc.parent_crm_account_tsp_region,
                sfdc.tsp_region,
                sfdc.crm_person_region
            ) as sfdc_parent_crm_account_tsp_region,
            iff(marketo_lead.email_address is not null, true, false) as is_marketo_lead,
            coalesce(
                marketo_lead.is_marketo_email_bounced, false
            ) as is_marketo_email_hard_bounced,
            marketo_lead.marketo_email_bounced_date as marketo_email_hard_bounced_date,
            coalesce(
                marketo_lead.is_marketo_unsubscribed, false
            ) as is_marketo_opted_out,
            marketo_lead.marketo_compliance_segment_value
            as marketo_compliance_segment_value,
            case
                when sfdc.email_address is not null then true else false
            end as is_sfdc_lead_contact,
            sfdc.sfdc_record_id,
            sfdc.dim_crm_account_id,
            sfdc.sfdc_lead_contact,
            coalesce(marketo_lead.mobile_phone, sfdc.mobile_phone) as mobile_phone,
            sfdc.sfdc_created_date as sfdc_created_date,
            sfdc.opted_out_salesforce as is_sfdc_opted_out,
            case
                when gitlab_dotcom.email_address is not null then true else false
            end as is_gitlab_dotcom_user,
            gitlab_dotcom.user_id as gitlab_dotcom_user_id,
            gitlab_dotcom.created_date as gitlab_dotcom_created_date,
            gitlab_dotcom.confirmed_date as gitlab_dotcom_confirmed_date,
            gitlab_dotcom.active_state as gitlab_dotcom_active_state,
            gitlab_dotcom.last_login_date as gitlab_dotcom_last_login_date,
            gitlab_dotcom.email_opted_in as gitlab_dotcom_email_opted_in,
            datediff(
                day, gitlab_dotcom.confirmed_date, getdate()
            ) as days_since_saas_signup,
            {{ days_buckets("days_since_saas_signup") }}
            as days_since_saas_signup_bucket,
            case
                when customer_db.email_address is not null then true else false
            end as is_customer_db_user,
            customer_db.customer_id as customer_db_customer_id,
            customer_db.created_date as customer_db_created_date,
            customer_db.confirmed_date as customer_db_confirmed_date,
            datediff(
                day, customer_db.confirmed_date, getdate()
            ) as days_since_self_managed_owner_signup,
            {{ days_buckets("days_since_self_managed_owner_signup") }}
            as days_since_self_managed_owner_signup_bucket,
            case
                when zuora.email_address is not null then true else false
            end as is_zuora_billing_contact,
            zuora.contact_id as zuora_contact_id,
            zuora.created_date as zuora_created_date,
            zuora.active_state as zuora_active_state,
            dnc_list.result as dnc_list_result,
            case
                when dnc_list.result in ('undeliverable', 'do_not_send')
                then false
                else true
            end as wip_is_valid_email_address,
            case
                when not wip_is_valid_email_address then dnc_list.result
            end as wip_invalid_email_address_reason

        from emails
        left join sfdc on sfdc.email_address = emails.email_address
        left join gitlab_dotcom on gitlab_dotcom.email_address = emails.email_address
        left join customer_db on customer_db.email_address = emails.email_address
        left join zuora on zuora.email_address = emails.email_address
        left join marketo_lead on marketo_lead.email_address = emails.email_address
        left join dnc_list on dnc_list.email_address = emails.email_address

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@rmistry",
            updated_by="@jpeguero",
            created_date="2021-01-19",
            updated_date="2022-04-07",
        )
    }}
