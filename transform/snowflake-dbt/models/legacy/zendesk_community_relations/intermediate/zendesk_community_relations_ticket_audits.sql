with
    source as (

        select *
        from {{ ref("zendesk_community_relations_ticket_audits_source") }}
        -- currently scoped to only sla_policy and priority
        where audit_field in ('sla_policy', 'priority', 'is_public', 'status')

    )

select *
from source
