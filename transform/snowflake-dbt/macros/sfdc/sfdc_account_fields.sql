{%- macro sfdc_account_fields(model_type) %}

with
    map_merged_crm_account as (

        select * from {{ ref("map_merged_crm_account") }} {%- if model_type == "live" %}

    {%- elif model_type == "snapshot" %}
    ),
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where
            date_actual >= '2020-03-01' and date_actual <= current_date
            {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            and date_id > (select max(snapshot_id) from {{ this }})

            {% endif %}
    {%- endif %}),
    sfdc_account as (

        select
            {%- if model_type == "live" %} *
            {%- elif model_type == "snapshot" %}
            {{
                dbt_utils.surrogate_key(
                    [
                        "sfdc_account_snapshots_source.account_id",
                        "snapshot_dates.date_id",
                    ]
                )
            }} as crm_account_snapshot_id,
            snapshot_dates.date_id as snapshot_id,
            sfdc_account_snapshots_source.*
            {%- endif %}
        from {%- if model_type == "live" %} {{ ref("sfdc_account_source") }}
        {%- elif model_type == "snapshot" %}
            {{ ref("sfdc_account_snapshots_source") }}
        inner join
            snapshot_dates
            on snapshot_dates.date_actual
            >= sfdc_account_snapshots_source.dbt_valid_from
            and snapshot_dates.date_actual
            < coalesce(
                sfdc_account_snapshots_source.dbt_valid_to, '9999-12-31'::timestamp
            )
        {%- endif %}
        where account_id is not null

    ),
    sfdc_users as (

        select
            {%- if model_type == "live" %} *
            {%- elif model_type == "snapshot" %}
            {{
                dbt_utils.surrogate_key(
                    ["sfdc_user_snapshots_source.user_id", "snapshot_dates.date_id"]
                )
            }} as crm_user_snapshot_id,
            snapshot_dates.date_id as snapshot_id,
            sfdc_user_snapshots_source.*
            {%- endif %}
        from {%- if model_type == "live" %} {{ ref("sfdc_users_source") }}
        {%- elif model_type == "snapshot" %}
            {{ ref("sfdc_user_snapshots_source") }}
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= sfdc_user_snapshots_source.dbt_valid_from
            and snapshot_dates.date_actual
            < coalesce(sfdc_user_snapshots_source.dbt_valid_to, '9999-12-31'::timestamp)
        {%- endif %}

    ),
    sfdc_record_type as (select * from {{ ref("sfdc_record_type") }}),
    ultimate_parent_account as (

        select
            {%- if model_type == "live" %}

            {%- elif model_type == "snapshot" %} crm_account_snapshot_id, snapshot_id,
            {%- endif %}
            account_id,
            account_name,
            billing_country,
            df_industry,
            industry,
            sub_industry,
            account_owner_team,
            tsp_territory,
            tsp_region,
            tsp_sub_region,
            tsp_area,
            gtm_strategy,
            tsp_account_employees,
            tsp_max_family_employees,
            account_demographics_sales_segment,
            account_demographics_geo,
            account_demographics_region,
            account_demographics_area,
            account_demographics_territory,
            account_demographics_employee_count,
            account_demographics_max_family_employee,
            account_demographics_upa_country,
            account_demographics_upa_state,
            account_demographics_upa_city,
            account_demographics_upa_street,
            account_demographics_upa_postal_code,
            created_date,
            zi_technologies,
            zoom_info_website,
            zoom_info_company_other_domains,
            zoom_info_dozisf_zi_id,
            zoom_info_parent_company_zi_id,
            zoom_info_parent_company_name,
            zoom_info_ultimate_parent_company_zi_id,
            zoom_info_ultimate_parent_company_name
        from sfdc_account
        where account_id = ultimate_parent_account_id

    ),
    final as (

        select
            -- crm account informtion
            {%- if model_type == "live" %}

            {%- elif model_type == "snapshot" %}
            sfdc_account.crm_account_snapshot_id, sfdc_account.snapshot_id,
            {%- endif %}
            sfdc_account.owner_id as dim_crm_user_id,
            sfdc_account.account_id as dim_crm_account_id,
            sfdc_account.account_name as crm_account_name,
            sfdc_account.billing_country as crm_account_billing_country,
            sfdc_account.account_type as crm_account_type,
            sfdc_account.industry as crm_account_industry,
            sfdc_account.sub_industry as crm_account_sub_industry,
            sfdc_account.account_owner as crm_account_owner,
            sfdc_account.account_owner_team as crm_account_owner_team,
            sfdc_account.tsp_territory as crm_account_sales_territory,
            sfdc_account.tsp_region as crm_account_tsp_region,
            sfdc_account.tsp_sub_region as crm_account_tsp_sub_region,
            sfdc_account.tsp_area as crm_account_tsp_area,
            sfdc_account.account_demographics_sales_segment
            as parent_crm_account_demographics_sales_segment,
            sfdc_account.account_demographics_geo
            as parent_crm_account_demographics_geo,
            sfdc_account.account_demographics_region
            as parent_crm_account_demographics_region,
            sfdc_account.account_demographics_area
            as parent_crm_account_demographics_area,
            sfdc_account.account_demographics_territory
            as parent_crm_account_demographics_territory,
            sfdc_account.account_demographics_employee_count
            as crm_account_demographics_employee_count,
            sfdc_account.account_demographics_max_family_employee
            as parent_crm_account_demographics_max_family_employee,
            sfdc_account.account_demographics_upa_country
            as parent_crm_account_demographics_upa_country,
            sfdc_account.account_demographics_upa_state
            as parent_crm_account_demographics_upa_state,
            sfdc_account.account_demographics_upa_city
            as parent_crm_account_demographics_upa_city,
            sfdc_account.account_demographics_upa_street
            as parent_crm_account_demographics_upa_street,
            sfdc_account.account_demographics_upa_postal_code
            as parent_crm_account_demographics_upa_postal_code,
            sfdc_account.gtm_strategy as crm_account_gtm_strategy,
            case
                when
                    lower(sfdc_account.gtm_strategy)
                    in (
                        'account centric',
                        'account based - net new',
                        'account based - expand'
                    )
                then 'Focus Account'
                else 'Non - Focus Account'
            end as crm_account_focus_account,
            sfdc_account.account_owner_user_segment as crm_account_owner_user_segment,
            sfdc_account.tsp_account_employees as crm_account_tsp_account_employees,
            sfdc_account.tsp_max_family_employees
            as crm_account_tsp_max_family_employees,
            case
                when sfdc_account.tsp_max_family_employees > 2000
                then 'Employees > 2K'
                when
                    sfdc_account.tsp_max_family_employees <= 2000
                    and sfdc_account.tsp_max_family_employees > 1500
                then 'Employees > 1.5K'
                when
                    sfdc_account.tsp_max_family_employees <= 1500
                    and sfdc_account.tsp_max_family_employees > 1000
                then 'Employees > 1K'
                else 'Employees < 1K'
            end as crm_account_employee_count_band,
            sfdc_account.health_score,
            sfdc_account.health_number,
            sfdc_account.health_score_color,
            sfdc_account.partner_account_iban_number,
            cast(
                sfdc_account.partners_signed_contract_date as date
            ) as partners_signed_contract_date,
            sfdc_account.record_type_id as record_type_id,
            sfdc_account.federal_account as federal_account,
            sfdc_account.is_jihu_account as is_jihu_account,
            sfdc_account.carr_this_account,
            sfdc_account.carr_account_family,
            sfdc_account.potential_arr_lam,
            sfdc_account.lam as parent_crm_account_lam,
            sfdc_account.lam_dev_count as parent_crm_account_lam_dev_count,
            sfdc_account.fy22_new_logo_target_list,
            sfdc_account.is_first_order_available,
            sfdc_account.gitlab_com_user,
            sfdc_account.tsp_account_employees,
            sfdc_account.tsp_max_family_employees,
            account_owner.name as account_owner,
            sfdc_users.name as technical_account_manager,
            sfdc_account.is_deleted as is_deleted,
            map_merged_crm_account.dim_crm_account_id as merged_to_account_id,
            iff(
                sfdc_record_type.record_type_label = 'Partner'
                and sfdc_account.partner_type in ('Alliance', 'Channel')
                and sfdc_account.partner_status = 'Authorized',
                true,
                false
            ) as is_reseller,
            sfdc_account.created_date as crm_account_created_date,
            sfdc_account.zi_technologies as crm_account_zi_technologies,
            sfdc_account.technical_account_manager_date,
            sfdc_account.zoom_info_website as crm_account_zoom_info_website,
            sfdc_account.zoom_info_company_other_domains
            as crm_account_zoom_info_company_other_domains,
            sfdc_account.zoom_info_dozisf_zi_id as crm_account_zoom_info_dozisf_zi_id,
            sfdc_account.zoom_info_parent_company_zi_id
            as crm_account_zoom_info_parent_company_zi_id,
            sfdc_account.zoom_info_parent_company_name
            as crm_account_zoom_info_parent_company_name,
            sfdc_account.zoom_info_ultimate_parent_company_zi_id
            as crm_account_zoom_info_ultimate_parent_company_zi_id,
            sfdc_account.zoom_info_ultimate_parent_company_name
            as crm_account_zoom_info_ultimate_parent_company_name,

            -- --ultimate parent crm account info
            ultimate_parent_account.account_id as dim_parent_crm_account_id,
            ultimate_parent_account.account_name as parent_crm_account_name,
            {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}
            as parent_crm_account_sales_segment,
            ultimate_parent_account.billing_country
            as parent_crm_account_billing_country,
            ultimate_parent_account.industry as parent_crm_account_industry,
            ultimate_parent_account.sub_industry as parent_crm_account_sub_industry,
            sfdc_account.parent_account_industry_hierarchy
            as parent_crm_account_industry_hierarchy,
            ultimate_parent_account.account_owner_team as parent_crm_account_owner_team,
            ultimate_parent_account.tsp_territory as parent_crm_account_sales_territory,
            ultimate_parent_account.tsp_region as parent_crm_account_tsp_region,
            ultimate_parent_account.tsp_sub_region as parent_crm_account_tsp_sub_region,
            ultimate_parent_account.tsp_area as parent_crm_account_tsp_area,
            ultimate_parent_account.gtm_strategy as parent_crm_account_gtm_strategy,
            case
                when
                    lower(ultimate_parent_account.gtm_strategy)
                    in (
                        'account centric',
                        'account based - net new',
                        'account based - expand'
                    )
                then 'Focus Account'
                else 'Non - Focus Account'
            end as parent_crm_account_focus_account,
            ultimate_parent_account.tsp_account_employees
            as parent_crm_account_tsp_account_employees,
            ultimate_parent_account.tsp_max_family_employees
            as parent_crm_account_tsp_max_family_employees,
            case
                when ultimate_parent_account.tsp_max_family_employees > 2000
                then 'Employees > 2K'
                when
                    ultimate_parent_account.tsp_max_family_employees <= 2000
                    and ultimate_parent_account.tsp_max_family_employees > 1500
                then 'Employees > 1.5K'
                when
                    ultimate_parent_account.tsp_max_family_employees <= 1500
                    and ultimate_parent_account.tsp_max_family_employees > 1000
                then 'Employees > 1K'
                else 'Employees < 1K'
            end as parent_crm_account_employee_count_band,
            ultimate_parent_account.created_date as parent_crm_account_created_date,
            ultimate_parent_account.zi_technologies
            as parent_crm_account_zi_technologies,
            ultimate_parent_account.zoom_info_website
            as parent_crm_account_zoom_info_website,
            ultimate_parent_account.zoom_info_company_other_domains
            as parent_crm_account_zoom_info_company_other_domains,
            ultimate_parent_account.zoom_info_dozisf_zi_id
            as parent_crm_account_zoom_info_dozisf_zi_id,
            ultimate_parent_account.zoom_info_parent_company_zi_id
            as parent_crm_account_zoom_info_parent_company_zi_id,
            ultimate_parent_account.zoom_info_parent_company_name
            as parent_crm_account_zoom_info_parent_company_name,
            ultimate_parent_account.zoom_info_ultimate_parent_company_zi_id
            as parent_crm_account_zoom_info_ultimate_parent_company_zi_id,
            ultimate_parent_account.zoom_info_ultimate_parent_company_name
            as parent_crm_account_zoom_info_ultimate_parent_company_name
        from sfdc_account
        left join
            map_merged_crm_account
            on sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
        left join
            sfdc_record_type
            on sfdc_account.record_type_id = sfdc_record_type.record_type_id
        {%- if model_type == "live" %}
        left join
            ultimate_parent_account
            on sfdc_account.ultimate_parent_account_id
            = ultimate_parent_account.account_id
        left outer join
            sfdc_users on sfdc_account.technical_account_manager_id = sfdc_users.user_id
        left join
            sfdc_users as account_owner on account_owner.user_id = sfdc_account.owner_id
        {%- elif model_type == "snapshot" %}
        left join
            ultimate_parent_account
            on sfdc_account.ultimate_parent_account_id
            = ultimate_parent_account.account_id
            and sfdc_account.snapshot_id = ultimate_parent_account.snapshot_id
        left outer join
            sfdc_users
            on sfdc_account.technical_account_manager_id = sfdc_users.user_id
            and sfdc_account.snapshot_id = sfdc_users.snapshot_id
        left join
            sfdc_users as account_owner
            on account_owner.user_id = sfdc_account.owner_id
            and account_owner.snapshot_id = sfdc_account.snapshot_id
        {%- endif %}

    )

{%- endmacro %}
