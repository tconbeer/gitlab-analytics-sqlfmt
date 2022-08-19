{{ config({"alias": "gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id"}) }}


with
    source as (

        select *
        from
            {{
                source(
                    "snapshots",
                    "gitlab_dotcom_gitlab_subscriptions_namespace_id_snapshots",
                )
            }}
        -- This ID has NULL values for many of the important columns.
        where id != 572635

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
            seats_in_use::number as seats_in_use,
            seats_owed::number as seats_owed,
            trial_extension_type::number as trial_extension_type,
            "DBT_VALID_FROM"::timestamp as valid_from,
            "DBT_VALID_TO"::timestamp as valid_to

        from source

    )

select *
from renamed
