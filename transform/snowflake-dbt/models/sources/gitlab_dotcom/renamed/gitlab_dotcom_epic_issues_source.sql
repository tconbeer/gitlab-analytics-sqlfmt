with
    source as (select * from {{ ref("gitlab_dotcom_epic_issues_dedupe_source") }}),
    renamed as (

        select
            id::number as epic_issues_relation_id,
            epic_id::number as epic_id,
            issue_id::number as issue_id,
            relative_position::number as epic_issue_relative_position

        from source

    )

select *
from renamed
