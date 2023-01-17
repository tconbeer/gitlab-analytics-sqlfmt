with
    source as (select * from {{ ref("gitlab_dotcom_services_dedupe_source") }}),
    renamed as (

        select
            id::number as service_id,
            type::varchar as service_type,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            active::boolean as is_active,
            properties::varchar as service_properties,
            template::boolean as service_template,
            push_events::boolean as has_push_events,
            issues_events::boolean as has_issues_events,
            merge_requests_events::boolean as has_merge_requests_events,
            tag_push_events::boolean as has_tag_push_events,
            note_events::boolean as has_note_events,
            category::varchar as service_catetgory,
            wiki_page_events::boolean as has_wiki_page_events,
            pipeline_events::boolean as has_pipeline_events,
            confidential_issues_events::boolean as has_confidential_issues_events,
            commit_events::boolean as has_commit_events,
            job_events::boolean as has_job_events,
            confidential_note_events::boolean as has_confidential_note_events,
            deployment_events::boolean as has_deployment_events,
            comment_on_event_enabled::boolean as is_comment_on_event_enabled
        from source

    )

select *
from renamed
