{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("xmau_metrics", "gitlab_dotcom_xmau_metrics"),
            ("usage_data_events", "gitlab_dotcom_usage_data_events"),
            ("dim_usage_ping_metric", "dim_usage_ping_metric"),
            ("dim_license", "dim_license"),
            ("dim_date", "dim_date"),
            ("namespace_order_subscription", "bdg_namespace_order_subscription"),
            ("dim_subscription", "dim_subscription"),
            ("dim_namespace", "dim_namespace"),
        ]
    )
}}

,
fct_events as (

    select
        event_primary_key as event_primary_key,
        usage_data_events.event_name as event_name,
        namespace_id as namespace_id,  -- make empty namespace = null
        'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' as dim_instance_id,
        user_id as user_id,
        parent_type as parent_type,
        parent_id as parent_id,
        iff(usage_data_events.parent_type = 'project', parent_id, null) as project_id,
        event_created_at as event_created_at,
        case
            when usage_data_events.stage_name is null
            then xmau_metrics.stage_name
            else usage_data_events.stage_name
        end as stage_name,
        group_name as group_name,
        section_name as section_name,
        smau as smau,
        gmau as gmau,
        is_umau as umau,
        project_is_learn_gitlab as project_is_learn_gitlab
    from usage_data_events
    left join
        xmau_metrics on usage_data_events.event_name = xmau_metrics.events_to_include

),
paid_flag_by_day as (

    select
        namespace_id,
        cast(event_created_at as date) as event_date,
        plan_was_paid_at_event_date as plan_was_paid_at_event_date,
        plan_id_at_event_date as plan_id_at_event_date,
        plan_name_at_event_date as plan_name_at_event_date,
        event_created_at
    from usage_data_events
    qualify
        row_number() over (
            partition by namespace_id, event_date order by event_created_at desc
        ) = 1

),
fct_events_w_plan_was_paid as (

    select
        fct_events.*,
        paid_flag_by_day.plan_was_paid_at_event_date as plan_was_paid_at_event_date,
        paid_flag_by_day.plan_id_at_event_date as plan_id_at_event_date,
        paid_flag_by_day.plan_name_at_event_date as plan_name_at_event_date
    from fct_events
    left join
        paid_flag_by_day
        on fct_events.namespace_id = paid_flag_by_day.namespace_id
        and cast(fct_events.event_created_at as date) = paid_flag_by_day.event_date

),
deduped_namespace_bdg as (

    select
        bdg.dim_subscription_id as dim_subscription_id,
        bdg.order_id as order_id,
        bdg.dim_crm_account_id as dim_crm_account_id,
        bdg.dim_billing_account_id as dim_billing_account_id,
        bdg.dim_namespace_id as dim_namespace_id
    from namespace_order_subscription as bdg
    inner join
        dim_subscription as ds on bdg.dim_subscription_id = ds.dim_subscription_id
    where
        product_tier_name_subscription in (
            'SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium'
        )
    qualify
        row_number() over (
            partition by dim_namespace_id order by subscription_version desc
        ) = 1

),
dim_namespace_w_bdg as (

    select
        dim_namespace.dim_namespace_id as dim_namespace_id,
        dim_namespace.dim_product_tier_id as dim_product_tier_id,
        deduped_namespace_bdg.dim_subscription_id as dim_subscription_id,
        deduped_namespace_bdg.order_id as order_id,
        deduped_namespace_bdg.dim_crm_account_id as dim_crm_account_id,
        deduped_namespace_bdg.dim_billing_account_id as dim_billing_account_id
    from deduped_namespace_bdg
    inner join
        dim_namespace
        on dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

),
final as (

    select *
    from fct_events_w_plan_was_paid
    left join
        dim_namespace_w_bdg
        on fct_events_w_plan_was_paid.namespace_id
        = dim_namespace_w_bdg.dim_namespace_id

),
gitlab_dotcom_fact as (

    select
        event_primary_key as event_id,
        event_name as event_name,
        dim_instance_id as dim_instance_id,
        dim_product_tier_id as dim_product_tier_id,
        dim_subscription_id as dim_subscription_id,
        date_id as dim_event_date_id,
        dim_crm_account_id as dim_crm_account_id,
        dim_billing_account_id as dim_billing_account_id,
        namespace_id as dim_namespace_id,
        project_id as dim_project_id,
        user_id as dim_user_id,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        event_created_at as event_created_at,
        plan_id_at_event_date as plan_id_at_event_date,
        plan_name_at_event_date as plan_name_at_event_date,
        plan_was_paid_at_event_date as plan_was_paid_at_event_date,
        project_is_learn_gitlab as project_is_learn_gitlab,
        'GITLAB_DOTCOM' as data_source
    from final
    left join dim_date on to_date(event_created_at) = dim_date.date_day

),
results as (select * from gitlab_dotcom_fact)

{{
    dbt_audit(
        cte_ref="results",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-01-20",
        updated_date="2022-02-10",
    )
}}
