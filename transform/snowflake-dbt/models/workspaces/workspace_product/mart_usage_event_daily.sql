{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_namespace", "dim_namespace"),
            ("fct_usage_event", "fct_usage_event"),
            ("xmau_metrics", "gitlab_dotcom_xmau_metrics"),
        ]
    )
}}

,
fact_with_date as (
    select
        event_id,
        cast(event_created_at as date) as event_date,
        dim_user_id as dim_user_id,
        event_name,
        plan_id_at_event_date,
        {{
            dbt_utils.surrogate_key(
                ["event_date", "dim_user_id", "dim_namespace_id", "event_name"]
            )
        }} as mart_usage_event_daily_id,
        dim_product_tier_id,
        dim_subscription_id,
        dim_crm_account_id,
        dim_billing_account_id,
        stage_name,
        section_name,
        group_name,
        data_source,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_namespace_id,
        dim_instance_id
    from fct_usage_event as fact

),
fact_with_namespace as (

    select
        fact.*,
        cast(namespace.created_at as date) as namespace_created_at,
        datediff(day, namespace_created_at, getdate()) as days_since_namespace_created,
        datediff(
            day, namespace_created_at, event_date
        ) as days_since_namespace_creation_at_event_date,
        namespace.namespace_is_internal as namespace_is_internal
    from fact_with_date as fact
    left join
        dim_namespace as namespace on fact.dim_namespace_id = namespace.dim_namespace_id

),
fact_with_xmau_flags as (

    select fact.*, xmau.smau as is_smau, xmau.gmau as is_gmau, xmau.is_umau as is_umau
    from fact_with_namespace as fact
    left join xmau_metrics as xmau on fact.event_name = xmau.events_to_include

),
results as (

    select
        mart_usage_event_daily_id,
        event_date,
        dim_user_id,
        dim_namespace_id,
        dim_instance_id,
        plan_id_at_event_date,
        event_name,
        dim_product_tier_id,
        dim_subscription_id,
        dim_crm_account_id,
        dim_billing_account_id,
        stage_name,
        section_name,
        group_name,
        data_source,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        namespace_is_internal,
        namespace_created_at,
        days_since_namespace_created,
        days_since_namespace_creation_at_event_date,
        is_smau,
        is_gmau,
        is_umau,
        count(*) as event_count
    from fact_with_xmau_flags {{ dbt_utils.group_by(n=24) }}

)

{{
    dbt_audit(
        cte_ref="results",
        created_by="@dihle",
        updated_by="@dihle",
        created_date="2022-02-11",
        updated_date="2022-02-15",
    )
}}
