with
    source as (

        select distinct user_id, issue_id
        from {{ source("gitlab_dotcom", "issue_assignees") }}

    ),
    renamed as (

        select
            md5(user_id || issue_id)::varchar as user_issue_relation_id,
            user_id::number as user_id,
            issue_id::number as issue_id
        from source

    )

select distinct *
from renamed
