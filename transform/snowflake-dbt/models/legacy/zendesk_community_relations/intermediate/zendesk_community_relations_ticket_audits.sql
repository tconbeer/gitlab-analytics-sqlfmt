WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_community_relations_ticket_audits_source') }}
    -- currently scoped to only sla_policy and priority
    WHERE audit_field IN ('sla_policy', 'priority', 'is_public', 'status')
    
)

SELECT *
FROM source
