with
    resource_weight_events as (

        select * from {{ ref("gitlab_dotcom_resource_weight_events") }}

    ),
    issues as (select * from {{ ref("gitlab_dotcom_issues") }}),
    joined as (

        select resource_weight_events.*, issues.project_id
        from resource_weight_events
        left join issues on resource_weight_events.issue_id = issues.issue_id

    )

select *
from joined
