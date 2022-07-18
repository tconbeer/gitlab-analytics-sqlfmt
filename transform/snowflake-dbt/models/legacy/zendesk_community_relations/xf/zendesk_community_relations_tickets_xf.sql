with
    zendesk_community_relations_tickets as (

        select * from {{ ref("zendesk_community_relations_tickets_source") }}

    ),
    zendesk_community_relations_ticket_metrics as (

        select
            ticket_id,
            solved_at,
            sla_reply_time_business_hours,
            sla_reply_time_calendar_hours
        from {{ ref("zendesk_community_relations_ticket_metrics") }}

    ),
    zendesk_community_relations_organizations as (

        select organization_id, organization_tags
        from {{ ref("zendesk_community_relations_organizations_source") }}

    ),
    zendesk_community_relations_groups as (

        select * from {{ ref("zendesk_community_relations_groups_source") }}

    ),
    zendesk_community_relations_users as (

        select * from {{ ref("zendesk_community_relations_users_source") }}

    )

select distinct
    zendesk_community_relations_tickets.*,
    zendesk_community_relations_ticket_metrics.sla_reply_time_business_hours,
    zendesk_community_relations_ticket_metrics.sla_reply_time_calendar_hours,
    zendesk_community_relations_groups.group_name as channel,
    zendesk_community_relations_users.name as assignee_name,
    zendesk_community_relations_users.role as assignee_role,
    zendesk_community_relations_organizations.organization_tags,
    zendesk_community_relations_ticket_metrics.solved_at
from zendesk_community_relations_tickets
left join
    zendesk_community_relations_ticket_metrics
    on zendesk_community_relations_tickets.ticket_id
    = zendesk_community_relations_ticket_metrics.ticket_id
left join
    zendesk_community_relations_organizations
    on zendesk_community_relations_tickets.organization_id
    = zendesk_community_relations_organizations.organization_id
left join
    zendesk_community_relations_groups
    on zendesk_community_relations_groups.group_id
    = zendesk_community_relations_tickets.group_id
left join
    zendesk_community_relations_users
    on zendesk_community_relations_users.user_id
    = zendesk_community_relations_tickets.assignee_id
