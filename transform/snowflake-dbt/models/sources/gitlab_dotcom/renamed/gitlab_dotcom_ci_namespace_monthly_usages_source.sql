with
    source as (

        select *
        from {{ ref("gitlab_dotcom_ci_namespace_monthly_usages_dedupe_source") }}

    )

    ,
    renamed as (
        select
            id::number as ci_namespace_monthly_usages_id,
            namespace_id::number as namespace_id,
            date::timestamp as date,
            additional_amount_available::number as additional_amount_available,
            amount_used::number as amount_used,
            notification_level::number as notification_level,
            shared_runners_duration::number as shared_runners_duration,
            created_at::timestamp as created_at
        from source

    )

select *
from renamed
