with
    source as (select * from {{ ref("gitlab_dotcom_events_dedupe_source") }}),
    renamed as (

        select
            id as event_id,
            project_id::number as project_id,
            author_id::number as author_id,
            target_id::number as target_id,
            target_type::varchar as target_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            action::number as event_action_type_id,
            {{ action_type(action_type_id="event_action_type_id") }}::varchar
            as event_action_type

        from source

    )

select *
from renamed
