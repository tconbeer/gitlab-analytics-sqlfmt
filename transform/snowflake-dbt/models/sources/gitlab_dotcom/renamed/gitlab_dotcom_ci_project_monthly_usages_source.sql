with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_project_monthly_usages_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_project_monthly_usages_id,
            project_id::number as project_id,
            date::timestamp as date,
            amount_used::number as amount_used,
            shared_runners_duration::number as shared_runners_duration,
            created_at::timestamp as created_at
        from source

    )

select *
from renamed
