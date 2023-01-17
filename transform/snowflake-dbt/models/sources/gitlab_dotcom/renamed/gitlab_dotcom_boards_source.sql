
with
    source as (select * from {{ ref("gitlab_dotcom_boards_dedupe_source") }}),
    renamed as (

        select
            id::number as board_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            milestone_id::number as milestone_id,
            group_id::number as group_id,
            weight::number as weight

        from source

    )

select *
from renamed
