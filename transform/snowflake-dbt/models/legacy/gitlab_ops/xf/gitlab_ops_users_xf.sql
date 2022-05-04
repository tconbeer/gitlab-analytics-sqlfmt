-- This data model code comes from
-- https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_dotcom/xf/gitlab_dotcom_users_xf.sql, except we're removed references to other tables that do not exist in the data warehouse
select
    {{ dbt_utils.star(from=ref('gitlab_ops_users'), except=["created_at", "first_name", "last_name", "notification_email", "public_email", "updated_at", "users_name"]) }},
    created_at as user_created_at,
    updated_at as user_updated_at,
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
    end as account_age_cohort
from {{ ref("gitlab_ops_users") }}
