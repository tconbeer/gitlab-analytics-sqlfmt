with
    source as (select * from {{ ref("gitlab_dotcom_sprints_dedupe_source") }}),
    parsed_columns as (

        select
            id::number as sprint_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            start_date::date as start_date,
            due_date::date as due_date,
            project_id::number as project_id,
            group_id::number as group_id,
            iid::number as sprint_iid,
            cached_markdown_version::number as cached_markdown_version,
            title::varchar as sprint_title,
            title_html::varchar as sprint_title_html,
            description::varchar as sprint_description,
            description_html::varchar as sprint_description_html,
            state_enum::number as sprint_state_enum
        from source

    )

select *
from parsed_columns
