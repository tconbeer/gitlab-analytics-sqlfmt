with
    source as (

        select * from {{ ref("gitlab_dotcom_approval_project_rules_groups_source") }}

    )

select *
from source
