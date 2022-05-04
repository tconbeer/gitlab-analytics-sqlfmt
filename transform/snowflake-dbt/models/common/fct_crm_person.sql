{{ config(tags=["mnpi_exception"]) }}

with
    account_dims_mapping as (select * from {{ ref("map_crm_account") }}),
    crm_person as (

        select

            dim_crm_person_id,
            sfdc_record_id,
            bizible_person_id,
            bizible_touchpoint_position,
            bizible_marketing_channel_path,
            bizible_touchpoint_date,
            last_utm_content,
            last_utm_campaign,
            dim_crm_account_id,
            dim_crm_user_id,
            person_score,
            name_of_active_sequence,
            sequence_task_due_date,
            sequence_status,
            last_activity_date,
            account_demographics_sales_segment,
            account_demographics_sales_segment_grouped,
            account_demographics_geo,
            account_demographics_region,
            account_demographics_area,
            account_demographics_segment_region_grouped,
            account_demographics_territory,
            account_demographics_employee_count,
            account_demographics_max_family_employee,
            account_demographics_upa_country,
            account_demographics_upa_state,
            account_demographics_upa_city,
            account_demographics_upa_street,
            account_demographics_upa_postal_code

        from {{ ref("prep_crm_person") }}

    ),
    industry as (select * from {{ ref("prep_industry") }}),
    bizible_marketing_channel_path as (

        select * from {{ ref("prep_bizible_marketing_channel_path") }}

    ),
    bizible_marketing_channel_path_mapping as (

        select * from {{ ref("map_bizible_marketing_channel_path") }}

    ),
    sales_segment as (select * from {{ ref("dim_sales_segment") }}),
    sales_territory as (select * from {{ ref("prep_sales_territory") }}),
    sfdc_contacts as (

        select * from {{ ref("sfdc_contact_source") }} where is_deleted = 'FALSE'

    ),
    sfdc_leads as (

        select * from {{ ref("sfdc_lead_source") }} where is_deleted = 'FALSE'

    ),
    sfdc_lead_converted as (

        select *
        from sfdc_leads
        where is_converted
        qualify
            row_number() over (
                partition by converted_contact_id order by converted_date desc
            ) = 1

    ),
    marketing_qualified_leads as (

        select

            {{
                dbt_utils.surrogate_key(
                    [
                        "COALESCE(converted_contact_id, lead_id)",
                        "marketo_qualified_lead_date::timestamp",
                    ]
                )
            }} as event_id,
            marketo_qualified_lead_date::timestamp as event_timestamp,
            lead_id as sfdc_record_id,
            'lead' as sfdc_record,
            {{ dbt_utils.surrogate_key(["COALESCE(converted_contact_id, lead_id)"]) }}
            as crm_person_id,
            converted_contact_id as contact_id,
            converted_account_id as account_id,
            owner_id as crm_user_id,
            person_score as person_score

        from sfdc_leads
        where marketo_qualified_lead_date is not null

    ),
    marketing_qualified_contacts as (

        select

            {{
                dbt_utils.surrogate_key(
                    ["contact_id", "marketo_qualified_lead_date::timestamp"]
                )
            }} as event_id,
            marketo_qualified_lead_date::timestamp as event_timestamp,
            contact_id as sfdc_record_id,
            'contact' as sfdc_record,
            {{ dbt_utils.surrogate_key(["contact_id"]) }} as crm_person_id,
            contact_id as contact_id,
            account_id as account_id,
            owner_id as crm_user_id,
            person_score as person_score

        from sfdc_contacts
        where marketo_qualified_lead_date is not null
        having event_id not in (select event_id from marketing_qualified_leads)

    ),
    mqls_unioned as (

        select *
        from marketing_qualified_leads

        union

        select *
        from marketing_qualified_contacts

    ),
    mqls as (

        select

            crm_person_id,
            min(event_timestamp) as first_mql_date,
            max(event_timestamp) as last_mql_date,
            count(*) as mql_count

        from mqls_unioned
        group by 1

    ),
    final as (

        select
            -- ids
            crm_person.dim_crm_person_id as dim_crm_person_id,
            crm_person.sfdc_record_id as sfdc_record_id,
            crm_person.bizible_person_id as bizible_person_id,

            -- common dimension keys
            crm_person.dim_crm_user_id as dim_crm_user_id,
            crm_person.dim_crm_account_id as dim_crm_account_id,
            -- dim_parent_crm_account_id
            account_dims_mapping.dim_parent_crm_account_id,
            coalesce(
                account_dims_mapping.dim_account_sales_segment_id,
                sales_segment.dim_sales_segment_id
            ) as dim_account_sales_segment_id,
            coalesce(
                account_dims_mapping.dim_account_sales_territory_id,
                sales_territory.dim_sales_territory_id
            ) as dim_account_sales_territory_id,
            coalesce(
                account_dims_mapping.dim_account_industry_id, industry.dim_industry_id
            ) as dim_account_industry_id,
            -- dim_account_location_country_id
            account_dims_mapping.dim_account_location_country_id,
            -- dim_account_location_region_id
            account_dims_mapping.dim_account_location_region_id,
            -- dim_parent_sales_segment_id
            account_dims_mapping.dim_parent_sales_segment_id,
            -- dim_parent_sales_territory_id
            account_dims_mapping.dim_parent_sales_territory_id,
            account_dims_mapping.dim_parent_industry_id,  -- dim_parent_industry_id
            -- dim_parent_location_country_id
            account_dims_mapping.dim_parent_location_country_id,
            -- dim_parent_location_region_id
            account_dims_mapping.dim_parent_location_region_id,
            {{
                get_keyed_nulls(
                    "bizible_marketing_channel_path.dim_bizible_marketing_channel_path_id"
                )
            }}
            as dim_bizible_marketing_channel_path_id,

            -- important person dates
            coalesce(
                sfdc_leads.created_date,
                sfdc_lead_converted.created_date,
                sfdc_contacts.created_date
            )::date as created_date,
            {{
                get_date_id(
                    "COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date, sfdc_contacts.created_date)"
                )
            }}
            as created_date_id,
            {{
                get_date_pt_id(
                    "COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date, sfdc_contacts.created_date)"
                )
            }}
            as created_date_pt_id,
            coalesce(
                sfdc_leads.created_date, sfdc_lead_converted.created_date
            )::date as lead_created_date,
            {{
                get_date_id(
                    "COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date)::DATE"
                )
            }}
            as lead_created_date_id,
            {{
                get_date_pt_id(
                    "COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date)::DATE"
                )
            }}
            as lead_created_date_pt_id,
            sfdc_contacts.created_date::date as contact_created_date,
            {{ get_date_id("sfdc_contacts.created_date::DATE") }}
            as contact_created_date_id,
            {{ get_date_pt_id("sfdc_contacts.created_date::DATE") }}
            as contact_created_date_pt_id,
            coalesce(
                sfdc_contacts.inquiry_datetime, sfdc_leads.inquiry_datetime
            )::date as inquiry_date,
            {{ get_date_id("inquiry_date") }} as inquiry_date_id,
            {{ get_date_pt_id("inquiry_date") }} as inquiry_date_pt_id,
            coalesce(
                sfdc_contacts.inquiry_datetime_inferred,
                sfdc_leads.inquiry_datetime_inferred
            )::date as inquiry_inferred_datetime,
            {{ get_date_id("inquiry_inferred_datetime") }}
            as inquiry_inferred_datetime_id,
            {{ get_date_pt_id("inquiry_inferred_datetime") }}
            as inquiry_inferred_datetime_pt_id,
            least(
                coalesce(inquiry_date, '9999-01-01'),
                coalesce(inquiry_inferred_datetime, '9999-01-01')
            ) as prep_true_inquiry_date,
            case
                when prep_true_inquiry_date != '9999-01-01' then prep_true_inquiry_date
            end as true_inquiry_date,
            mqls.first_mql_date::date as mql_date_first,
            mqls.first_mql_date as mql_datetime_first,
            convert_timezone(
                'America/Los_Angeles', mqls.first_mql_date
            ) as mql_datetime_first_pt,
            {{ get_date_id("mql_date_first") }} as mql_date_first_id,
            {{ get_date_pt_id("mql_date_first") }} as mql_date_first_pt_id,
            mqls.last_mql_date::date as mql_date_latest,
            mqls.last_mql_date as mql_datetime_latest,
            convert_timezone(
                'America/Los_Angeles', mqls.last_mql_date
            ) as mql_datetime_latest_pt,
            {{ get_date_id("last_mql_date") }} as mql_date_latest_id,
            {{ get_date_pt_id("last_mql_date") }} as mql_date_latest_pt_id,
            coalesce(
                sfdc_contacts.marketo_qualified_lead_datetime,
                sfdc_leads.marketo_qualified_lead_datetime
            )::date as mql_sfdc_date,
            coalesce(
                sfdc_contacts.marketo_qualified_lead_datetime,
                sfdc_leads.marketo_qualified_lead_datetime
            ) as mql_sfdc_datetime,
            {{ get_date_id("mql_sfdc_date") }} as mql_sfdc_date_id,
            {{ get_date_pt_id("mql_sfdc_date") }} as mql_sfdc_date_pt_id,
            coalesce(
                sfdc_contacts.mql_datetime_inferred, sfdc_leads.mql_datetime_inferred
            )::date as mql_inferred_date,
            coalesce(
                sfdc_contacts.mql_datetime_inferred, sfdc_leads.mql_datetime_inferred
            ) as mql_inferred_datetime,
            {{ get_date_id("mql_inferred_date") }} as mql_inferred_date_id,
            {{ get_date_pt_id("mql_inferred_date") }} as mql_inferred_date_pt_id,
            coalesce(
                sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime
            )::date as accepted_date,
            coalesce(
                sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime
            ) as accepted_datetime,
            convert_timezone(
                'America/Los_Angeles',
                coalesce(sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime)
            ) as accepted_datetime_pt,
            {{ get_date_id("accepted_date") }} as accepted_date_id,
            {{ get_date_pt_id("accepted_date") }} as accepted_date_pt_id,
            coalesce(
                sfdc_contacts.qualifying_datetime, sfdc_leads.qualifying_datetime
            )::date as qualifying_date,
            {{ get_date_id("qualifying_date") }} as qualifying_date_id,
            {{ get_date_pt_id("qualifying_date") }} as qualifying_date_pt_id,
            coalesce(
                sfdc_contacts.qualified_datetime, sfdc_leads.qualified_datetime
            )::date as qualified_date,
            {{ get_date_id("qualified_date") }} as qualified_date_id,
            {{ get_date_pt_id("qualified_date") }} as qualified_date_pt_id,
            sfdc_lead_converted.converted_date::date as converted_date,
            {{ get_date_id("sfdc_lead_converted.converted_date") }}
            as converted_date_id,
            {{ get_date_pt_id("sfdc_lead_converted.converted_date") }}
            as converted_date_pt_id,
            coalesce(
                sfdc_contacts.worked_datetime, sfdc_leads.worked_datetime
            )::date as worked_date,
            {{ get_date_id("worked_date") }} as worked_date_id,
            {{ get_date_pt_id("worked_date") }} as worked_date_pt_id,

            -- flags
            case when mqls.first_mql_date is not null then 1 else 0 end as is_mql,
            case when true_inquiry_date is not null then 1 else 0 end as is_inquiry,


            -- information fields
            crm_person.name_of_active_sequence,
            crm_person.sequence_task_due_date,
            crm_person.sequence_status,
            crm_person.last_activity_date,
            crm_person.last_utm_content,
            crm_person.last_utm_campaign,
            crm_person.account_demographics_sales_segment,
            crm_person.account_demographics_sales_segment_grouped,
            crm_person.account_demographics_geo,
            crm_person.account_demographics_region,
            crm_person.account_demographics_area,
            crm_person.account_demographics_segment_region_grouped,
            crm_person.account_demographics_territory,
            crm_person.account_demographics_employee_count,
            crm_person.account_demographics_max_family_employee,
            crm_person.account_demographics_upa_country,
            crm_person.account_demographics_upa_state,
            crm_person.account_demographics_upa_city,
            crm_person.account_demographics_upa_street,
            crm_person.account_demographics_upa_postal_code,

            -- additive fields
            crm_person.person_score as person_score,
            mqls.mql_count as mql_count


        from crm_person
        left join sfdc_leads on crm_person.sfdc_record_id = sfdc_leads.lead_id
        left join sfdc_contacts on crm_person.sfdc_record_id = sfdc_contacts.contact_id
        left join
            sfdc_lead_converted
            on crm_person.sfdc_record_id = sfdc_lead_converted.converted_contact_id
        left join mqls on crm_person.dim_crm_person_id = mqls.crm_person_id
        left join
            account_dims_mapping
            on crm_person.dim_crm_account_id = account_dims_mapping.dim_crm_account_id
        left join
            sales_segment
            on sfdc_leads.sales_segmentation = sales_segment.sales_segment_name
        left join
            sales_territory
            on sfdc_leads.tsp_territory = sales_territory.sales_territory_name
        left join
            industry on coalesce(
                sfdc_contacts.industry, sfdc_leads.industry
            ) = industry.industry_name
        left join
            bizible_marketing_channel_path_mapping
            on crm_person.bizible_marketing_channel_path
            = bizible_marketing_channel_path_mapping.bizible_marketing_channel_path
        left join
            bizible_marketing_channel_path
            on bizible_marketing_channel_path_mapping.bizible_marketing_channel_path_name_grouped
            = bizible_marketing_channel_path.bizible_marketing_channel_path_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2020-12-01",
            updated_date="2022-03-26",
        )
    }}
