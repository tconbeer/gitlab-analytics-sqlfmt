with
    source as (select * from {{ ref("gitlab_dotcom_milestones_dedupe_source") }}),
    renamed as (

        select

            id::number as milestone_id,
            title::varchar as milestone_title,
            description::varchar as milestone_description,
            project_id::number as project_id,
            group_id::number as group_id,
            start_date::date as start_date,
            due_date::date as due_date,
            state::varchar as milestone_status,

            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
