with
    source as (select * from {{ ref("gitlab_dotcom_lists_dedupe_source") }}),
    renamed as (

        select
            id::number as list_id,
            board_id::number as board_id,
            label_id::number as label_id,
            list_type::number as list_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            user_id::number as user_id,
            milestone_id::number as milestone_id,
            max_issue_count::number as max_issue_count,
            max_issue_weight::number as max_issue_weight,
            limit_metric::varchar as limit_metric
        from source

    )

select *
from renamed
