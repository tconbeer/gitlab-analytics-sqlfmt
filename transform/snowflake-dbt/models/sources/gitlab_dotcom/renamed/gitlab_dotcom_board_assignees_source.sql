with
    source as (select * from {{ ref("gitlab_dotcom_board_assignees_dedupe_source") }}),
    renamed as (

        select
            id::number as board_assignee_relation_id,
            board_id::number as board_id,
            assignee_id::number as board_assignee_id

        from source

    )

select *
from renamed
