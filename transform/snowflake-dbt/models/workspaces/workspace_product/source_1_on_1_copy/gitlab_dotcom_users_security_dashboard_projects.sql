with
    source as (

        select *
        from {{ ref("gitlab_dotcom_users_security_dashboard_projects_source") }}

    )

select *
from source
