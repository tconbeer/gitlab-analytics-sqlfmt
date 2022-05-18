with
    source as (

        select *
        from {{ ref("gitlab_dotcom_approval_project_rules_groups_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_rules_groups_id,
            approval_project_rule_id::number as approval_project_rule_id,
            group_id::number as group_id

        from source

    )

select *
from renamed
