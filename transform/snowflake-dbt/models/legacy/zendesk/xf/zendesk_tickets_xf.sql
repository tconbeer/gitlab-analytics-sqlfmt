with
    zendesk_tickets as (

        select
            {{
                dbt_utils.star(
                    from=ref("zendesk_tickets_source"), except=["custom_fields"]
                )
            }}
        from {{ ref("zendesk_tickets_source") }}

    ),
    zendesk_users_source as (select * from {{ ref("zendesk_users_source") }}),
    zendesk_ticket_metrics as (

        select
            ticket_id,
            solved_at,
            least(
                sla_reply_time_business_hours, sla_reply_time_calendar_hours
            ) as first_reply_time
        from {{ ref("zendesk_ticket_metrics") }}

    ),
    zendesk_sla_policies as (

        select distinct
            zendesk_sla_policy_id,
            zendesk_sla_title,
            policy_metrics_business_hours,
            policy_metrics_priority,
            policy_metrics_target
        from {{ ref("zendesk_sla_policies_source") }}
        where policy_metrics_metric = 'first_reply_time'

    ),
    zendesk_organizations as (

        select
            organization_id,
            sfdc_account_id,
            organization_tags,
            organization_market_segment
        from {{ ref("zendesk_organizations_source") }}

    ),
    zendesk_tickets_sla as (select * from {{ ref("zendesk_tickets_sla_xf") }}),
    zendesk_satisfaction_ratings as (

        select * from {{ ref("zendesk_satisfaction_ratings_source") }}

    ),
    zendesk_ticket_audit_first_comment as (

        select
            ticket_id,
            audit_created_at as sla_audit_created_at,
            iff(audit_value = '0', false, true) as is_first_comment_public

        from {{ ref("zendesk_ticket_audits") }}
        where audit_field = 'is_public'
        qualify
            row_number() OVER (partition by ticket_id order by sla_audit_created_at) = 1

    )

select distinct
    zendesk_tickets.*,
    zendesk_satisfaction_ratings.created_at as satisfaction_rating_created_at,
    zendesk_ticket_metrics.first_reply_time,
    zendesk_organizations.sfdc_account_id,
    zendesk_organizations.organization_market_segment,
    zendesk_organizations.organization_tags,
    zendesk_tickets_sla.priority as ticket_priority_at_first_reply,
    zendesk_tickets_sla.sla_policy as ticket_sla_policy_at_first_reply,
    zendesk_tickets_sla.first_reply_time_sla,
    zendesk_tickets_sla.first_reply_at,
    zendesk_ticket_metrics.solved_at,
    zendesk_sla_policies.policy_metrics_target,
    iff(
        zendesk_tickets_sla.first_reply_time_sla
        <= zendesk_sla_policies.policy_metrics_target,
        true,
        false
    ) as was_support_sla_met,
    iff(
        zendesk_users_submitter.role = 'end-user'
        or (
            zendesk_users_submitter.role in ('agent', 'admin')
            and zendesk_ticket_audit_first_comment.is_first_comment_public = false
        ),
        true,
        false
    ) as is_part_of_sla,
    iff(
        zendesk_tickets_sla.first_reply_time_sla
        <= zendesk_sla_policies.policy_metrics_target,
        true,
        false
    ) as was_sla_achieved,
    iff(
        zendesk_tickets_sla.first_reply_time_sla
        > zendesk_sla_policies.policy_metrics_target,
        true,
        false
    ) as was_sla_breached

from zendesk_tickets
left join
    zendesk_ticket_metrics
    on zendesk_tickets.ticket_id = zendesk_ticket_metrics.ticket_id
left join
    zendesk_organizations
    on zendesk_tickets.organization_id = zendesk_organizations.organization_id
left join
    zendesk_tickets_sla on zendesk_tickets.ticket_id = zendesk_tickets_sla.ticket_id
left join
    zendesk_sla_policies
    on zendesk_tickets_sla.priority = zendesk_sla_policies.policy_metrics_priority
    and zendesk_tickets_sla.sla_policy = zendesk_sla_policies.zendesk_sla_title
left join
    zendesk_users_source as zendesk_users_submitter
    on zendesk_users_submitter.user_id = zendesk_tickets.submitter_id
left join
    zendesk_satisfaction_ratings
    on zendesk_satisfaction_ratings.satisfaction_rating_id
    = zendesk_tickets.satisfaction_rating_id
left join
    zendesk_ticket_audit_first_comment
    on zendesk_ticket_audit_first_comment.ticket_id = zendesk_tickets.ticket_id
