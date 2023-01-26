with
    source as (

        select *
        from {{ ref("gitlab_dotcom_approval_project_rules_users_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_rules_users_id,
            approval_project_rule_id::number as approval_project_rule_id,
            user_id::number as user_id

        from source

    )

select *
from renamed
