{%- macro sfdc_user_fields(model_type) %}

with
    sfdc_user_roles as (

        select * from {{ ref("sfdc_user_roles_source") }} {%- if model_type == "live" %}

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
    final as (

        select
            {%- if model_type == "live" %}

            {%- elif model_type == "snapshot" %}
            sfdc_users.crm_user_snapshot_id, sfdc_users.snapshot_id,
            {%- endif %}
            sfdc_users.user_id as dim_crm_user_id,
            sfdc_users.employee_number,
            sfdc_users.name as user_name,
            sfdc_users.title,
            sfdc_users.department,
            sfdc_users.team,
            sfdc_users.manager_id,
            sfdc_users.is_active,
            sfdc_users.start_date,
            sfdc_users.user_role_id,
            sfdc_users.user_role_type,
            sfdc_user_roles.name as user_role_name,
            {{ dbt_utils.surrogate_key(["sfdc_users.user_segment"]) }}
            as dim_crm_user_sales_segment_id,
            sfdc_users.user_segment as crm_user_sales_segment,
            sfdc_users.user_segment_grouped as crm_user_sales_segment_grouped,
            {{ dbt_utils.surrogate_key(["sfdc_users.user_geo"]) }}
            as dim_crm_user_geo_id,
            sfdc_users.user_geo as crm_user_geo,
            {{ dbt_utils.surrogate_key(["sfdc_users.user_region"]) }}
            as dim_crm_user_region_id,
            sfdc_users.user_region as crm_user_region,
            {{ dbt_utils.surrogate_key(["sfdc_users.user_area"]) }}
            as dim_crm_user_area_id,
            sfdc_users.user_area as crm_user_area,
            coalesce(
                sfdc_users.user_segment_geo_region_area,
                concat(
                    sfdc_users.user_segment,
                    '-',
                    sfdc_users.user_geo,
                    '-',
                    sfdc_users.user_region,
                    '-',
                    sfdc_users.user_area
                )
            ) as crm_user_sales_segment_geo_region_area,
            sfdc_users.user_segment_region_grouped
            as crm_user_sales_segment_region_grouped,
            created_date
        from sfdc_users
        left join sfdc_user_roles on sfdc_users.user_role_id = sfdc_user_roles.id

    )

{%- endmacro %}
