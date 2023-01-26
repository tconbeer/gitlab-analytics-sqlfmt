with
    source as (select * from {{ ref("gitlab_dotcom_subscriptions_dedupe_source") }}),
    renamed as (

        select

            id::number as subscription_id,
            user_id::number as user_id,
            subscribable_id::number as subscribable_id,
            project_id::number as project_id,
            subscribable_type,
            subscribed::boolean as is_subscribed,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
