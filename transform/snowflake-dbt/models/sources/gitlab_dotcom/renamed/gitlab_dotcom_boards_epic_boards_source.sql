with
    source as (

        select * from {{ ref("gitlab_dotcom_boards_epic_boards_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as epic_board_id,
            hide_backlog_list::boolean as hide_backlog_list,
            hide_closed_list::boolean as hide_closed_list,
            group_id::number as group_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
