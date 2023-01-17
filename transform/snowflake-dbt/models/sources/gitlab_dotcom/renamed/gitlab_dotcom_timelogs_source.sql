with
    source as (select * from {{ ref("gitlab_dotcom_timelogs_dedupe_source") }}),
    renamed as (

        select
            id::number as timelog_id,
            created_at::timestamp as created_at,
            spent_at::timestamp as spent_at,
            updated_at::timestamp as updated_at,

            time_spent::number as seconds_spent,

            issue_id::number as issue_id,
            merge_request_id::number as merge_request_id,
            user_id::number as user_id
        from source

    )

select *
from renamed
