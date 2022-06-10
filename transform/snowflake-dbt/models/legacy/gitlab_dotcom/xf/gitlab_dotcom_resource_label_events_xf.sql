with
    resource_label_events as (

        select * from {{ ref("gitlab_dotcom_resource_label_events") }}

    )

    ,
    epics as (select * from {{ ref("gitlab_dotcom_epics") }})

    ,
    issues as (select * from {{ ref("gitlab_dotcom_issues_xf") }})

    ,
    mrs as (select * from {{ ref("gitlab_dotcom_merge_requests_xf") }})

    ,
    joined as (

        select
            resource_label_events.*,
            coalesce(
                epics.group_id, issues.namespace_id, mrs.namespace_id
            ) as namespace_id
        from resource_label_events
        left join epics on resource_label_events.epic_id = epics.epic_id
        left join issues on resource_label_events.issue_id = issues.issue_id
        left join mrs on resource_label_events.merge_request_id = mrs.merge_request_id

    )

select *
from joined
