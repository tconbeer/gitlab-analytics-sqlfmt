{{ config(tags=["mnpi_exception"]) }}

{%- set smau_metrics = dbt_utils.get_query_results_as_dict(
    "SELECT        stage_name || '_' || sql_friendly_name   AS name,        sql_friendly_path                        AS path     FROM "
    ~ ref("dim_key_xmau_metric")
    ~ " WHERE is_smau     ORDER BY name"
) -%}

with
    prep_usage_ping as (

        select *
        from {{ ref("prep_usage_ping_subscription_mapped") }}
        where license_md5 is not null

    ),
    pivoted as (

        select

            {{ default_usage_ping_information() }}

            -- subscription_info
            is_usage_ping_license_in_licensedot,
            dim_license_id,
            dim_subscription_id,
            is_license_mapped_to_subscription,
            is_license_subscription_id_valid,
            dim_crm_account_id,
            dim_parent_crm_account_id,
            dim_location_country_id,

            {%- for metric in smau_metrics.PATH %}
            {{ metric }} as {{ smau_metrics.NAME[loop.index0] }}
            {%- if not loop.last %},{% endif -%}
            {% endfor %}

        from prep_usage_ping

    )

    {{
        dbt_audit(
            cte_ref="pivoted",
            created_by="@ischweickartDD",
            updated_by="@michellecooper",
            created_date="2021-03-15",
            updated_date="2021-04-27",
        )
    }}
