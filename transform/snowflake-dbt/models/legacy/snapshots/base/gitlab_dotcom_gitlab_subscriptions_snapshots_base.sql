{{ config({"alias": "gitlab_dotcom_gitlab_subscriptions_snapshots"}) }}

with
    source as (

        select *
        from {{ source("snapshots", "gitlab_dotcom_gitlab_subscriptions_snapshots") }}
        where
            id
            != 572635  -- This ID has NULL values for many of the important columns.
            and namespace_id is not null

    ),
    renamed as (

        select
            dbt_scd_id::varchar as gitlab_subscription_snapshot_id,
            id::number as gitlab_subscription_id,
            start_date::date as gitlab_subscription_start_date,
            end_date::date as gitlab_subscription_end_date,
            trial_ends_on::date as gitlab_subscription_trial_ends_on,
            namespace_id::number as namespace_id,
            hosted_plan_id::number as plan_id,
            max_seats_used::number as max_seats_used,
            seats::number as seats,
            trial::boolean as is_trial,
            created_at::timestamp as gitlab_subscription_created_at,
            updated_at::timestamp as gitlab_subscription_updated_at,
            "DBT_VALID_FROM"::timestamp as valid_from,
            "DBT_VALID_TO"::timestamp as valid_to

        from source

    )

select *
from renamed
