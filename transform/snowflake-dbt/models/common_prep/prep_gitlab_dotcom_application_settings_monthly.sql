-- > First date in dim_date, used to back-fill the initial default settings for CI
-- minutes and storage limits.
{%- set first_ = dbt_utils.get_query_results_as_dict("SELECT MIN(date_actual) AS date FROM " ~ ref('dim_date')) -%}
{%- set first_ci_minute_limit = 2000 -%}
{%- set first_repository_storage_limit = 10737418240 -%}  -- > 1 GiB, in bytes.
-- > Date that the default limit for CI minutes was updated from 2000 to 400.
{%- set first_ci_minute_limit_change_date = "2020-10-01" -%}
-- > This will be used to identify the row we need to back-fill to
-- {{first_ci_minute_limit_change_date}} since the snapshot table was created several
-- month after the default setting was updated.
{%- set first_app_settings_snapshot_id = "442ab0695cfd8a3fa7ffbddb959903ad" -%}

{{
    simple_cte(
        [
            ("app_settings", "gitlab_dotcom_application_settings_snapshots_base"),
            ("dates", "dim_date"),
        ]
    )
}}

,
application_settings_historical as (

    select
        application_settings_snapshot_id,
        iff(
            application_settings_snapshot_id = '{{  first_app_settings_snapshot_id  }}',
            '{{  first_ci_minute_limit_change_date  }}',
            valid_from
        ) as valid_from,
        ifnull(valid_to, current_timestamp) as valid_to,
        application_settings_id,
        shared_runners_minutes,
        repository_size_limit
    from app_settings

    union all

    select
        md5('-1') as application_settings_snapshot_id,
        '{{  first_.DATE[0]  }}' as valid_from,
        dateadd('ms', -1, '{{  first_ci_minute_limit_change_date  }}') as valid_to,
        -1 as application_settings_id,
        {{ first_ci_minute_limit }} as shared_runners_minutes,
        {{ first_repository_storage_limit }} as repository_size_limit

),
application_settings_snapshot_monthly as (

    select
        date_trunc('month', dates.date_actual) as snapshot_month,
        application_settings_historical.application_settings_id,
        application_settings_historical.shared_runners_minutes,
        application_settings_historical.repository_size_limit
    from application_settings_historical
    inner join
        dates
        on dates.date_actual between application_settings_historical.valid_from
        and application_settings_historical.valid_to
    qualify row_number() over (partition by snapshot_month order by valid_from desc) = 1

),
keyed as (

    select
        {{ dbt_utils.surrogate_key(["snapshot_month", "application_settings_id"]) }}
        as primary_key,
        *
    from application_settings_snapshot_monthly

)

{{
    dbt_audit(
        cte_ref="keyed",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-03-30",
        updated_date="2021-03-30",
    )
}}
