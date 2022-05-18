{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_namespace", "dim_namespace"),
            ("dim_date", "dim_date"),
            ("fct_usage_event", "fct_usage_event"),
            ("xmau_metrics", "gitlab_dotcom_xmau_metrics"),
        ]
    )
}}

,
fact_raw as (

    select
        event_id,
        cast(event_created_at as date) as event_date,
        dim_user_id,
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
        dim_namespace_id,
        date_trunc('MONTH', event_date) as reporting_month,
        quarter(event_date) as reporting_quarter,
        year(event_date) as reporting_year

    from fct_usage_event as fact

),
fact_with_date_range as (

    select
        fact.event_id,
        fact.event_date,
        dim_date.last_day_of_month as last_day_of_month,
        dim_date.last_day_of_quarter as last_day_of_quarter,
        dim_date.last_day_of_fiscal_year as last_day_of_fiscal_year,
        fact.dim_user_id,
        fact.event_name,
        fact.dim_product_tier_id,
        fact.dim_subscription_id,
        fact.dim_crm_account_id,
        fact.dim_billing_account_id,
        fact.stage_name,
        fact.section_name,
        fact.group_name,
        fact.data_source,
        fact.plan_id_at_event_date,
        fact.plan_name_at_event_date,
        fact.plan_was_paid_at_event_date,
        fact.dim_namespace_id,
        fact.reporting_month,
        fact.reporting_quarter,
        fact.reporting_year
    from fact_raw as fact
    left join dim_date on fact.event_date = dim_date.date_actual
    where
        fact.event_date between dateadd(
            'day', -27, last_day_of_month
        ) and last_day_of_month

),
fact_with_namespace as (

    select
        fact.*,
        cast(namespace.created_at as date) as namespace_created_at,
        datediff(day, namespace_created_at, getdate()) as days_since_namespace_created
    from fact_with_date_range as fact
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
        {{
            dbt_utils.surrogate_key(
                ["reporting_month", "plan_id_at_event_date", "event_name"]
            )
        }} as mart_usage_event_plan_monthly_id,
        reporting_month,
        plan_id_at_event_date,
        event_name,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_umau,
        -- reporting_quarter, (commented out to reduce table size. If want to look at
        -- reporting_year,     quarter or yearly usage, uncomment and add to surrogate
        -- key)
        count(*) as event_count,
        count(distinct(dim_namespace_id)) as namespace_count,
        count(distinct(dim_user_id)) as user_count
    from fact_with_xmau_flags {{ dbt_utils.group_by(n=10) }}
    order by reporting_month desc, plan_id_at_event_date desc

)

{{
    dbt_audit(
        cte_ref="results",
        created_by="@dihle",
        updated_by="@dihle",
        created_date="2022-02-22",
        updated_date="2022-02-23",
    )
}}
