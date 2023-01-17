with
    source as (select * from {{ ref("gitlab_dotcom_audit_events_dedupe_source") }}),
    renamed as (

        select
            id::number as audit_event_id,
            author_id::number as author_id,
            entity_id::number as entity_id,
            entity_type::varchar as entity_type,
            details::varchar as audit_event_details,
            created_at::timestamp as created_at
        from source

    )

select *
from renamed
