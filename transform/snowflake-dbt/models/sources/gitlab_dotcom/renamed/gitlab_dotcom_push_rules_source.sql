with
    source as (select * from {{ ref("gitlab_dotcom_push_rules_dedupe_source") }}),
    renamed as (

        select
            id::number as push_rule_id,
            force_push_regex::varchar as force_push_regex,
            delete_branch_regex::varchar as delete_branch_regex,
            commit_message_regex::varchar as commit_message_regex,
            deny_delete_tag::boolean as deny_delete_tag,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            author_email_regex::varchar as author_email_regex,
            member_check::boolean as has_member_check,
            file_name_regex::varchar as file_name_regex,
            is_sample::boolean as is_sample,
            max_file_size::number as max_file_size,
            branch_name_regex::varchar as branch_name_regex,
            commit_message_negative_regex::varchar as commit_message_negative_regex
        from source

    )

select *
from renamed
