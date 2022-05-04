{{ config(tags=["mnpi_exception"]) }}

with
    customers as (select * from {{ ref("customers_db_customers") }}),
    trials as (select * from {{ ref("customers_db_trials") }}),
    users as (

        select
            {{ dbt_utils.star(from=ref('gitlab_dotcom_users'), except=["created_at", "first_name", "last_name", "notification_email", "public_email", "updated_at", "users_name"]) }},
            created_at as user_created_at,
            updated_at as user_updated_at
        from {{ ref("gitlab_dotcom_users") }}

    ),
    highest_paid_subscription_plan as (

        select * from {{ ref("gitlab_dotcom_highest_paid_subscription_plan") }}

    ),
    customers_with_trial as (

        select
            customers.customer_provider_user_id as user_id,
            min(customers.customer_id) as first_customer_id,
            min(customers.customer_created_at) as first_customer_created_at,
            array_agg(customers.customer_id)
            within group(order  by customers.customer_id) as customer_id_list,
            max(iff(order_id is not null, true, false)) as has_started_trial,
            min(trial_start_date) as has_started_trial_at
        from customers
        left join trials on customers.customer_id = trials.customer_id
        where customers.customer_provider = 'gitlab'
        group by 1

    ),
    joined as (
        select
            users.*,
            timestampdiff(days, user_created_at, last_activity_on) as days_active,
            timestampdiff(days, user_created_at, current_timestamp(2)) as account_age,
            case
                when account_age <= 1
                then '1 - 1 day or less'
                when account_age <= 7
                then '2 - 2 to 7 days'
                when account_age <= 14
                then '3 - 8 to 14 days'
                when account_age <= 30
                then '4 - 15 to 30 days'
                when account_age <= 60
                then '5 - 31 to 60 days'
                when account_age > 60
                then '6 - Over 60 days'
            end as account_age_cohort,

            highest_paid_subscription_plan.highest_paid_subscription_plan_id,
            highest_paid_subscription_plan.highest_paid_subscription_plan_is_paid
            as is_paid_user,
            highest_paid_subscription_plan.highest_paid_subscription_namespace_id,
            highest_paid_subscription_plan.highest_paid_subscription_ultimate_parent_id,
            highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_type,
            highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_id,

            iff(
                customers_with_trial.first_customer_id is not null, true, false
            ) as has_customer_account,
            customers_with_trial.first_customer_created_at,
            customers_with_trial.first_customer_id,
            customers_with_trial.customer_id_list,
            customers_with_trial.has_started_trial,
            customers_with_trial.has_started_trial_at

        from users
        left join
            highest_paid_subscription_plan
            on users.user_id = highest_paid_subscription_plan.user_id
        left join
            customers_with_trial
            on users.user_id::varchar = customers_with_trial.user_id::varchar
        where {{ filter_out_active_users("users", "user_id") }}

    )

select *
from joined
