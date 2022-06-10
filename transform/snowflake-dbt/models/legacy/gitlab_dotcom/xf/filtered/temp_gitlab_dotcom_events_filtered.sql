{{
    config(
        materialized="incremental", cluster_by=["target_type", "event_action_type_id"]
    )
}}

select
    event_action_type,
    event_action_type_id,
    target_type,
    created_at,
    author_id,
    project_id,
    event_id
from {{ ref("gitlab_dotcom_events") }}
where
    created_at is not null and created_at >= dateadd(month, -25, current_date) and (
        (target_type is null and event_action_type_id = 5) or
        (target_type = 'DesignManagement::Design' and event_action_type_id in (1, 2)) or
        (target_type = 'WikiPage::Meta' and event_action_type_id in (1, 2)) or
        (event_action_type = 'pushed')
    )

    {% if is_incremental() %}

    and created_at > (select max(created_at) from {{ this }})

    {% endif %}
