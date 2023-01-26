with
    issues as (

        select *
        from {{ ref("gitlab_dotcom_issues_xf") }}
        where
            project_id
            = 16492321  -- Recruiting for Open Positions

    ),
    users as (select * from {{ ref("gitlab_dotcom_users") }}),
    assignee as (select * from {{ ref("gitlab_dotcom_issue_assignees") }}),
    agg_assignee as (

        select
            issue_id,
            array_agg(lower(user_name)) within group (
                order by user_name asc
            ) as assignee
        from assignee
        left join users on assignee.user_id = users.user_id
        group by issue_id

    ),
    intermediate as (

        select
            issues.issue_title,
            issues.issue_iid,
            issues.issue_created_at,
            date_trunc(week, issue_created_at) as issue_created_week,
            issues.issue_closed_at,
            date_trunc(week, issues.issue_closed_at) as issue_closed_week,
            iff(issue_closed_at is not null, 1, 0) as is_issue_closed,
            issues.state as issue_state,
            agg_assignee.assignee,
            issues.issue_description,
            split_part(
                issue_description, '**Weekly Check-In Table**', 2
            ) as issue_description_split,
            case
                when
                    contains(
                        issue_description,
                        '[x] Yes, Diversity Sourcing methods were used'::varchar
                    )
                    = true
                then 'Used Diversity Strings'
                when
                    contains(
                        issue_description,
                        '[x] No, I did not use Diversity Sourcing methods'::varchar
                    )
                    = true
                then 'Did not use'
                when
                    contains(issue_description, '[x] Not Actively Sourcing'::varchar)
                    = true
                then 'Not Actively Sourcing'
                else 'No Answer'
            end as issue_answer
        from issues
        left join agg_assignee on agg_assignee.issue_id = issues.issue_id
        where
            lower(issue_title) like '%weekly check-in:%'
            and lower(issue_title) not like '%test%'

    ),
    split_issue as (

        select *
        from
            intermediate as splittable,
            lateral split_to_table(splittable.issue_description_split, '#### <summary>')
    -- -- splitting by year (numeric values)
    ),
    cleaned as (

        select
            *,
            left(trim(value), 10) as week_of,
            case
                when contains(value, '[x] Yes, Diversity sourcing was used')
                then 'Yes'
                when contains(value, '[x] Not actively sourcing')
                then 'Not Actively Sourcing'
                when contains(value, '[x] No, Did not use')
                then 'No'
                else issue_answer
            end as used_diversity_string
        from split_issue

    ),
    final as (

        select
            issue_title,
            issue_iid,
            issue_created_at,
            issue_created_week,
            issue_closed_at,
            issue_closed_week,
            week_of,
            -- -moved to using 1 issue per req and tracking weeks in issue on
            -- 2020.11.01
            is_issue_closed,
            issue_state,
            issue_description,
            assignee,
            used_diversity_string
        from cleaned

    )

select *
from final
