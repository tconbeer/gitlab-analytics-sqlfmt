
with
    source as (

        select *
        from {{ ref("gitlab_dotcom_approval_merge_request_rules_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as approval_merge_request_rule_id,
            merge_request_id::number as merge_request_id,
            approvals_required::number as is_approvals_required,
            rule_type::varchar as rule_type,
            report_type::varchar as report_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
