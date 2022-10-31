with
    source as (

        select *
        from {{ ref("gitlab_dotcom_users_security_dashboard_projects_dedupe_source") }}

    ),
    renamed as (

        select user_id::number as user_id, project_id::varchar as project_id from source

    )

select *
from renamed
