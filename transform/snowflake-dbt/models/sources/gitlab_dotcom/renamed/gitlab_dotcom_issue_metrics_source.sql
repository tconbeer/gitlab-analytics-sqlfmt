with
    source as (select * from {{ ref("gitlab_dotcom_issue_metrics_dedupe_source") }}),
    renamed as (

        select

            id::number as issue_metric_id,
            issue_id::number as issue_id,
            first_mentioned_in_commit_at::date as first_mentioned_in_commit_at,
            first_associated_with_milestone_at::date
            as first_associated_with_milestone_at,
            first_added_to_board_at::date as first_added_to_board_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
