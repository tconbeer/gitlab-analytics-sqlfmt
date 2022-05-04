{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("pi_targets", "performance_indicators_yaml_historical"),
            ("dim_date", "dim_date"),
        ]
    )
}},
first_day_of_month as (

    select distinct first_day_of_month as reporting_month from dim_date

),
most_recent_yml_record as (
    -- just grabs the most recent record of each metrics_path that dont have a null
    -- estimated target
    select *
    from pi_targets
    where pi_monthly_estimated_targets is not null
    qualify
        row_number() over (partition by pi_metric_name order by snapshot_date desc) = 1

),
flattened_monthly_targets as (
    -- flatten the json record from the yml file to get the target value and end month
    -- for each key:value pair
    select
        pi_metric_name, parse_json(d.path) [0]::timestamp as target_end_month, d.value
    from
        most_recent_yml_record,
        lateral flatten(
            input => parse_json(pi_monthly_estimated_targets), outer => true
        ) d

),
monthly_targets_with_intervals as (
    -- Calculate the reporting intervals for the pi_metric_name. Each row will have a
    -- start and end date
    select
        *,
        -- check if the row above the current row has a target_end_date:
        -- TRUE: then the start month = target_end_date from previous ROW_NUMBER
        -- FALSE: then make the start_month a year ago from TODAY
        coalesce(
            lag(target_end_month) over (partition by 1 order by target_end_month),
            dateadd('month', -12, current_date)
        ) as target_start_month
    from flattened_monthly_targets

),
final_targets as (
    -- join each metric_name and value to the reporting_month it corresponds WITH
    -- join IF reporting_month greater than metric start_month and reporting_month
    -- less than or equal to the target end month/ CURRENT_DATE
    select
        {{ dbt_utils.surrogate_key(["reporting_month", "pi_metric_name"]) }}
        as fct_performance_indicator_targets_id,
        reporting_month,
        pi_metric_name,
        value as target_value
    from first_day_of_month
    inner join monthly_targets_with_intervals
    where
        reporting_month > target_start_month and reporting_month <= coalesce(
            target_end_month, current_date
        )

),
results as (select * from final_targets)


{{
    dbt_audit(
        cte_ref="results",
        created_by="@dihle",
        updated_by="@dihle",
        created_date="2022-04-20",
        updated_date="2022-04-20",
    )
}}
