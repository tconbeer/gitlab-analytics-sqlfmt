{%- set stage_names = dbt_utils.get_column_values(
    ref("prep_stages_to_include_spo"), "stage_name", default=[]
) -%}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("dim_namespace", "dim_namespace"),
            ("prep_gitlab_dotcom_plan", "prep_gitlab_dotcom_plan"),
            ("dim_date", "dim_date"),
            ("all_events", "fct_daily_event_400"),
            ("metrics", "map_saas_event_to_smau"),
        ]
    )
}},
events as (

    select
        all_events.ultimate_parent_namespace_id,
        all_events.event_created_date,
        date_trunc('month', event_created_date) as event_month,
        prep_gitlab_dotcom_plan.plan_name as plan_name_at_reporting_month,
        dim_user_id,
        metrics.stage_name,
        count(event_created_date) as event_count,
        0 as umau
    from all_events
    inner join metrics on all_events.event_name = metrics.event_name and is_smau
    left join
        prep_gitlab_dotcom_plan
        on all_events.dim_plan_id_at_event_date = prep_gitlab_dotcom_plan.dim_plan_id
    where
        namespace_is_internal = false and days_since_namespace_creation >= 0
        {{ dbt_utils.group_by(n=6) }}

),
joined as (

    select
        'SaaS' as delivery,
        events.ultimate_parent_namespace_id as organization_id,
        namespace_type as organization_type,
        date(created_at) as organization_creation_date,
        first_day_of_month as reporting_month,
        stage_name,
        plan_name_at_reporting_month,
        iff(
            plan_name_at_reporting_month not in ('free', 'trial'), true, false
        ) as plan_is_paid,
        sum(event_count) as monthly_stage_events,
        count(distinct dim_user_id) as monthly_stage_users,
        count(distinct event_created_date) as stage_active_days,
        iff(monthly_stage_users > 0, true, false) as is_active_stage
    from events
    inner join dim_date on events.event_month = dim_date.date_day
    inner join
        dim_namespace
        on dim_namespace.dim_namespace_id = events.ultimate_parent_namespace_id
    where
        event_created_date >= dateadd('day', -28, dim_date.last_day_of_month)
        {{ dbt_utils.group_by(n=7) }}

)

select
    reporting_month,
    organization_id::varchar as organization_id,
    delivery,
    organization_type,
    plan_name_at_reporting_month as product_tier,
    plan_is_paid as is_paid_product_tier,
    -- organization_creation_date,
    -- created_by_blocked_user,
    sum(iff(is_active_stage > 0, 1, 0)) as active_stage_count,
    {{
        dbt_utils.pivot(
            "stage_name",
            stage_names,
            agg="MAX",
            then_value="monthly_stage_users",
            else_value="NULL",
            suffix="_stage",
            quote_identifiers=False,
        )
    }}
from joined {{ dbt_utils.group_by(n=6) }}
