{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_namespace", "dim_namespace"),
            ("xmau_metrics", "gitlab_dotcom_xmau_metrics"),
            ("dim_date", "dim_date"),
            ("fct_usage_event", "fct_usage_event"),
            ("xmau_metrics", "gitlab_dotcom_xmau_metrics"),
        ]
    )
}},
fact_raw as (

    select
        event_id,
        cast(event_created_at as date) as event_date,
        event_created_at,
        dim_user_id,
        fct_usage_event.event_name,
        data_source as data_source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_namespace_id,
        is_umau,
        gmau as is_gmau,
        smau as is_smau,
        fct_usage_event.section_name,
        fct_usage_event.stage_name,
        fct_usage_event.group_name,
        date_trunc('MONTH', event_date) as reporting_month,
        quarter(event_date) as reporting_quarter,
        year(event_date) as reporting_year
    from fct_usage_event
    left join
        xmau_metrics on fct_usage_event.event_name = xmau_metrics.events_to_include
    where is_umau = true or is_gmau = true or is_smau = true

),
fact_with_date_range as (

    select
        fact.event_id,
        fact.event_date,
        fact.event_created_at,
        dim_date.last_day_of_month as last_day_of_month,
        dim_date.last_day_of_quarter as last_day_of_quarter,
        dim_date.last_day_of_fiscal_year as last_day_of_fiscal_year,
        fact.dim_user_id,
        fact.event_name,
        fact.data_source,
        fact.plan_was_paid_at_event_date,
        fact.dim_namespace_id,
        fact.is_umau,
        fact.is_gmau,
        fact.is_smau,
        fact.section_name,
        fact.stage_name,
        fact.group_name,
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
paid_flag_by_month as (

    select dim_namespace_id, reporting_month, plan_was_paid_at_event_date
    from fact_with_date_range
    qualify
        row_number() over (
            partition by dim_namespace_id, reporting_month
            order by event_created_at desc
        )
        = 1

),
fact_w_paid_deduped as (

    select
        fact_with_date_range.event_id,
        fact_with_date_range.event_date,
        fact_with_date_range.last_day_of_month,
        fact_with_date_range.last_day_of_quarter,
        fact_with_date_range.last_day_of_fiscal_year,
        fact_with_date_range.dim_user_id,
        fact_with_date_range.event_name,
        fact_with_date_range.data_source,
        fact_with_date_range.dim_namespace_id,
        fact_with_date_range.is_umau,
        fact_with_date_range.is_gmau,
        fact_with_date_range.is_smau,
        fact_with_date_range.section_name,
        fact_with_date_range.stage_name,
        fact_with_date_range.group_name,
        fact_with_date_range.reporting_month,
        fact_with_date_range.reporting_quarter,
        fact_with_date_range.reporting_year,
        paid_flag_by_month.plan_was_paid_at_event_date
    from fact_with_date_range
    left join
        paid_flag_by_month
        on fact_with_date_range.dim_namespace_id = paid_flag_by_month.dim_namespace_id
        and fact_with_date_range.reporting_month = paid_flag_by_month.reporting_month

),
total_results as (

    select
        reporting_month,
        is_umau,
        is_gmau,
        is_smau,
        section_name,
        stage_name,
        group_name,
        'total' as user_group,
        array_agg(distinct event_name) within group (
            order by event_name
        ) as event_name_array,
        count(*) as event_count,
        count(distinct(dim_namespace_id)) as namespace_count,
        count(distinct(dim_user_id)) as user_count
    from fact_w_paid_deduped {{ dbt_utils.group_by(n=8) }}
    order by reporting_month desc

),
free_results as (

    select
        reporting_month,
        is_umau,
        is_gmau,
        is_smau,
        section_name,
        stage_name,
        group_name,
        'free' as user_group,
        array_agg(distinct event_name) within group (
            order by event_name
        ) as event_name_array,
        count(*) as event_count,
        count(distinct(dim_namespace_id)) as namespace_count,
        count(distinct(dim_user_id)) as user_count
    from fact_w_paid_deduped
    where plan_was_paid_at_event_date = false {{ dbt_utils.group_by(n=8) }}
    order by reporting_month desc

),
paid_results as (

    select
        reporting_month,
        is_umau,
        is_gmau,
        is_smau,
        section_name,
        stage_name,
        group_name,
        'paid' as user_group,
        array_agg(distinct event_name) within group (
            order by event_name
        ) as event_name_array,
        count(*) as event_count,
        count(distinct(dim_namespace_id)) as namespace_count,
        count(distinct(dim_user_id)) as user_count
    from fact_w_paid_deduped
    where plan_was_paid_at_event_date = true {{ dbt_utils.group_by(n=8) }}
    order by reporting_month desc

),
results_wo_pk as (

    select *
    from total_results
    union all
    select *
    from free_results
    union all
    select *
    from paid_results

),
results as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "reporting_month",
                    "user_group",
                    "section_name",
                    "stage_name",
                    "group_name",
                ]
            )
        }} as mart_xmau_metric_monthly_id, *
    from results_wo_pk

)

{{
    dbt_audit(
        cte_ref="results",
        created_by="@icooper_acp",
        updated_by="@icooper_acp",
        created_date="2022-02-23",
        updated_date="2022-03-03",
    )
}}
