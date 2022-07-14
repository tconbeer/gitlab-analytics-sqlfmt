{%- set stage_names = dbt_utils.get_column_values(
    ref("wk_prep_stages_to_include"), "stage_name", default=[]
) -%}

{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("date_details", "date_details"),
            ("blocked_users", "gitlab_dotcom_users_blocked_xf"),
            ("all_events", "gitlab_dotcom_daily_usage_data_events"),
            ("metrics", "gitlab_dotcom_xmau_metrics"),
        ]
    )
}},
all_namespaces as (

    select namespace_id, namespace_type, creator_id, namespace_created_at
    from {{ ref("gitlab_dotcom_namespaces_xf") }}
    where namespace_id = namespace_ultimate_parent_id and namespace_is_internal = false

),
namespaces as (

    select
        all_namespaces.*,
        iff(blocked_users.user_id is not null, true, false) as created_by_blocked_user
    from all_namespaces
    left join blocked_users on all_namespaces.creator_id = blocked_users.user_id

),
events as (

    select
        all_events.namespace_id,
        event_date,
        date_trunc('month', event_date) as event_month,
        plan_name_at_event_date,
        user_id,
        all_events.stage_name,
        iff(all_events.stage_name = 'manage', user_id, null) as umau,
        first_value(plan_name_at_event_date) OVER (
            partition by event_month, all_events.namespace_id order by event_date asc
        ) as plan_name_at_reporting_month,
        first_value(plan_name_at_event_date) OVER (
            partition by all_events.namespace_id order by event_date asc
        ) as plan_name_at_creation,
        count(event_date) as event_count
    from all_events
    inner join metrics on all_events.event_name = metrics.events_to_include
    where
        (metrics.smau = true or metrics.is_umau = true)
        and all_events.stage_name != 'monitor'
        and namespace_is_internal = false
        and days_since_namespace_creation >= 0
        {{ dbt_utils.group_by(n=7) }}

),
joined as (

    select
        'SaaS' as delivery,
        namespaces.namespace_id as organization_id,
        namespace_type as organization_type,
        date(namespace_created_at) as organization_creation_date,
        first_day_of_month as reporting_month,
        stage_name,
        plan_name_at_reporting_month,
        created_by_blocked_user,
        iff(
            plan_name_at_reporting_month in ('free', 'trial'), true, false
        ) as plan_is_paid,
        sum(event_count) as monthly_stage_events,
        count(distinct user_id) as monthly_stage_users,
        count(distinct event_date) as stage_active_days,
        count(distinct umau) as umau_stage,
        sum(umau_stage) OVER (
            partition by
                organization_id, first_day_of_month, plan_name_at_reporting_month
        ) as umau,
        iff(monthly_stage_users > 0, true, false) as is_active_stage
    from events
    inner join date_details on events.event_month = date_details.date_day
    inner join namespaces on namespaces.namespace_id = events.namespace_id
    where
        event_date >= dateadd('day', -28, date_details.last_day_of_month)
        and stage_name != 'manage'
        {{ dbt_utils.group_by(n=9) }}

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
    umau as umau_value,
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
from joined {{ dbt_utils.group_by(n=7) }}
