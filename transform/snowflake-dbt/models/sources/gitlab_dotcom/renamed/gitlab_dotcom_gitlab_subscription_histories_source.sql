with
    source as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscription_histories_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as gitlab_subscription_history_id,
            gitlab_subscription_created_at::timestamp as gitlab_subscription_created_at,
            gitlab_subscription_updated_at::timestamp as gitlab_subscription_updated_at,
            start_date::timestamp as start_date,
            end_date::timestamp as end_date,
            trial_starts_on::timestamp as trial_starts_on,
            trial_ends_on::timestamp as trial_ends_on,
            namespace_id::number as namespace_id,
            hosted_plan_id::number as hosted_plan_id,
            max_seats_used::number as max_seats_used,
            seats::number as seats,
            trial::boolean as is_trial,
            change_type::number as change_type,
            gitlab_subscription_id::number as gitlab_subscription_id,
            created_at::timestamp as created

        from source

    )

select *
from renamed
