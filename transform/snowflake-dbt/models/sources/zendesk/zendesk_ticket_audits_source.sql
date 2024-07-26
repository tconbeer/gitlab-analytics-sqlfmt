with
    source as (select * from {{ source("zendesk", "ticket_audits") }}),

    flattened as (

        select
            -- primary data
            source.id as audit_id,
            source.created_at as audit_created_at,
            -- foreign keys
            source.author_id as author_id,
            source.ticket_id as ticket_id,
            -- logical data
            flat_events.value['field_name'] as audit_field,
            flat_events.value['type'] as audit_type,
            flat_events.value['value'] as audit_value,
            flat_events.value['id'] as audit_event_id

        from
            source,
            lateral flatten(input => parse_json(events), outer => false) flat_events
        -- currently scoped to only sla_policy and priority
        where flat_events.value['field_name'] in ('sla_policy', 'priority', 'is_public')

    )

select *
from flattened
