{{ config({"schema": "legacy"}) }}

with
    users as (

        select *
        from {{ ref("gitlab_dotcom_users") }} users
        where {{ filter_out_blocked_users("users", "user_id") }}

    ),
    cohorting as (

        select
            user_id,
            created_at::date as cohort_date,
            timestampdiff(months, created_at, last_activity_on) as period
        from users

    ),
    joined as (

        select
            date_trunc('month', cohorting.cohort_date) as cohort_date,
            cohorting.period,
            count(distinct cohorting.user_id) as active_in_period_distinct_count,
            count(distinct base_cohort.user_id) as base_cohort_count,
            active_in_period_distinct_count / base_cohort_count::float as retention

        from cohorting
        join
            cohorting as base_cohort
            on cohorting.cohort_date = base_cohort.cohort_date
            and base_cohort.period = 0
        where cohorting.period is not null and cohorting.period >= 0
        group by 1, 2
        order by cohort_date desc

    )

select md5(cohort_date || period) as cohort_key, *
from joined
