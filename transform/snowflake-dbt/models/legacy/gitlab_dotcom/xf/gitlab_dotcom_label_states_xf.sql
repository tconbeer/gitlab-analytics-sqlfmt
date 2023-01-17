with
    resource_label_events as (

        select *
        from {{ ref("gitlab_dotcom_resource_label_events") }}
        where label_id is not null

    ),

    aggregated as (

        select
            label_id,

            epic_id,
            issue_id,
            merge_request_id,

            max(case when action_type = 'added' then created_at end) as max_added_at,
            max(case when action_type = 'removed' then created_at end) as max_removed_at

        from resource_label_events {{ dbt_utils.group_by(n=4) }}

    ),

    final as (  -- Leave removed_at NULL if less than added_at.

        select
            label_id,
            epic_id,
            issue_id,
            merge_request_id,
            max_added_at as added_at,
            case
                when max_removed_at > max_added_at
                then max_removed_at
                when max_added_at is null
                then max_removed_at
            end as removed_at,
            iff(removed_at is null, 'added', 'removed') as latest_state
        from aggregated
    )

select *
from final
