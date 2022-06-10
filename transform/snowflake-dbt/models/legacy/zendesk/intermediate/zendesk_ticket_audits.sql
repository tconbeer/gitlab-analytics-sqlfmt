{{ config({"schema": "legacy"}) }}

with
    source as (

        select *
        from {{ ref("zendesk_ticket_audits_source") }}
        -- currently scoped to only sla_policy and priority
        where audit_field in ('sla_policy', 'priority', 'is_public')

    )

select *
from source
