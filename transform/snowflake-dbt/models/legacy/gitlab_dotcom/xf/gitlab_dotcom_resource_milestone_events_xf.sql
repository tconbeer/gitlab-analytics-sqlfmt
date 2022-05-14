with
    gitlab_dotcom_resource_milestone_events as (

        select * from {{ ref("gitlab_dotcom_resource_milestone_events") }}

    )

    ,
    issues as (select * from {{ ref("gitlab_dotcom_issues") }})

    ,
    mrs as (select * from {{ ref("gitlab_dotcom_merge_requests") }})

    ,
    joined as (

        select
            gitlab_dotcom_resource_milestone_events.*,
            coalesce(issues.project_id, mrs.project_id) as project_id
        from gitlab_dotcom_resource_milestone_events
        left join
            issues on gitlab_dotcom_resource_milestone_events.issue_id = issues.issue_id
        left join
            mrs
            on gitlab_dotcom_resource_milestone_events.merge_request_id
            = mrs.merge_request_id

    )

select *
from joined
