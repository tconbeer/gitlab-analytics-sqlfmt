{%- set stage_names = dbt_utils.get_column_values(
    ref("wk_prep_stages_to_include"), "stage_name", default=[]
) -%}

{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{ simple_cte([("fct_monthly_usage_data", "fct_monthly_usage_data")]) }},
smau_only as (

    select
        host_name,
        dim_instance_id,
        {{ dbt_utils.surrogate_key(["host_name", "dim_instance_id"]) }}
        as organization_id,
        dim_usage_ping_id as dim_usage_ping_id,
        stage_name,
        ping_created_month,
        monthly_metric_value
    from fct_monthly_usage_data
    where is_smau = true

),
fct_usage_ping_payload as (select * from {{ ref("fct_usage_ping_payload") }})

select
    smau_only.ping_created_month as reporting_month,
    smau_only.organization_id,
    'Self-Managed' as delivery,
    iff(instance_user_count = 1, 'Individual', 'Group') as organization_type,
    fct_usage_ping_payload.product_tier,
    iff(
        fct_usage_ping_payload.product_tier <> 'Core', true, false
    ) as is_paid_product_tier,
    umau_value,
    count(
        distinct iff(monthly_metric_value > 0, stage_name, null)
    ) as active_stage_count,
    {{
        dbt_utils.pivot(
            "stage_name",
            stage_names,
            agg="MAX",
            then_value="monthly_metric_value",
            else_value="NULL",
            suffix="_stage",
            quote_identifiers=False,
        )
    }}
from smau_only
left join
    fct_usage_ping_payload
    on smau_only.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
    {{ dbt_utils.group_by(n=7) }}
