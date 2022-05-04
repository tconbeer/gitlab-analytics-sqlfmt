WITH zendesk_community_relations_tickets AS (

  SELECT *
  FROM {{ref('zendesk_community_relations_tickets_source')}}

), zendesk_community_relations_ticket_metrics AS (

  SELECT
    ticket_id,
    solved_at,
    sla_reply_time_business_hours,
    sla_reply_time_calendar_hours
  FROM {{ref('zendesk_community_relations_ticket_metrics')}}

), zendesk_community_relations_organizations AS (
 
  SELECT
    organization_id,
    organization_tags
  FROM {{ref('zendesk_community_relations_organizations_source')}}

), zendesk_community_relations_groups AS (

  SELECT *
  FROM {{ ref('zendesk_community_relations_groups_source') }}

), zendesk_community_relations_users AS (

  SELECT *
  FROM {{ ref('zendesk_community_relations_users_source') }}

)

SELECT DISTINCT 
  zendesk_community_relations_tickets.*,
  zendesk_community_relations_ticket_metrics.sla_reply_time_business_hours,
  zendesk_community_relations_ticket_metrics.sla_reply_time_calendar_hours,
  zendesk_community_relations_groups.group_name                             AS channel,
  zendesk_community_relations_users.name                                    AS assignee_name,
  zendesk_community_relations_users.role                                    AS assignee_role,
  zendesk_community_relations_organizations.organization_tags,
  zendesk_community_relations_ticket_metrics.solved_at
FROM zendesk_community_relations_tickets
LEFT JOIN zendesk_community_relations_ticket_metrics
  ON zendesk_community_relations_tickets.ticket_id = zendesk_community_relations_ticket_metrics.ticket_id
LEFT JOIN zendesk_community_relations_organizations
  ON zendesk_community_relations_tickets.organization_id = zendesk_community_relations_organizations.organization_id
LEFT JOIN zendesk_community_relations_groups
  ON zendesk_community_relations_groups.group_id = zendesk_community_relations_tickets.group_id
LEFT JOIN zendesk_community_relations_users
  ON zendesk_community_relations_users.user_id = zendesk_community_relations_tickets.assignee_id
