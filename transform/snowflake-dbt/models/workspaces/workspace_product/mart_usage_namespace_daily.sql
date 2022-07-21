{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("mart_usage_event", "mart_usage_event"),
        ]
    )
}},
usage_events as (
    select
        {{ dbt_utils.surrogate_key(["event_date", "event_name", "dim_namespace_id"]) }}
        as mart_usage_namespace_id,
        event_date,
        event_name,
        dim_product_tier_id,
        dim_subscription_id,
        dim_crm_account_id,
        dim_billing_account_id,
        stage_name,
        section_name,
        group_name,
        data_source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_instance_id,
        dim_namespace_id,
        namespace_created_at,
        days_since_namespace_created,
        is_smau,
        is_gmau,
        is_umau,
        count(*) as event_count,
        count(distinct(dim_user_id)) as distinct_user_count
    from mart_usage_event {{ dbt_utils.group_by(n=21) }}
),
results as (select * from usage_events)



{{
    dbt_audit(
        cte_ref="results",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-02-15",
        updated_date="2022-02-16",
    )
}}
