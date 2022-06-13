with
    zendesk_ticket_metrics as (

        select
            ticket_id,
            sla_reply_time_calendar_hours,
            sla_reply_time_business_hours,
            created_at
        from {{ ref("zendesk_ticket_metrics") }}

    ),
    zendesk_ticket_audit_sla as (

        /* ranking each audit and event within an audit to later select the last
       state of sla_policy and priority prior to first_reply_time */
        select
            ticket_id,
            dense_rank() over (
                partition by ticket_id order by audit_created_at desc
            ) as sla_audit_rank,
            dense_rank() over (
                partition by audit_id order by audit_event_id desc
            ) as sla_audit_event_rank,
            audit_created_at as sla_audit_created_at,
            audit_value as sla_policy
        from {{ ref("zendesk_ticket_audits") }}
        where audit_field = 'sla_policy'

    ),
    zendesk_ticket_audit_priority as (

        select
            ticket_id,
            dense_rank() over (
                partition by ticket_id order by audit_created_at desc
            ) as priority_audit_rank,
            dense_rank() over (
                partition by audit_id order by audit_event_id desc
            ) as priority_audit_event_rank,
            audit_created_at as priority_audit_created_at,
            audit_value as priority
        from {{ ref("zendesk_ticket_audits") }}
        where audit_field = 'priority'

    ),
    zendesk_ticket_emergency_sla_policy as (

        select
            ticket_id,
            iff(sla_policy = 'Emergency SLA', true, false) as is_emergency_sla
        from zendesk_ticket_audit_sla
        where sla_audit_rank = 1 and sla_audit_event_rank = 1

    ),
    zendesk_ticket_reply_time as (

        select
            zendesk_ticket_metrics.ticket_id,
            zendesk_ticket_metrics.created_at,
            iff(
                zendesk_ticket_emergency_sla_policy.is_emergency_sla = false,
                zendesk_ticket_metrics.sla_reply_time_business_hours,
                zendesk_ticket_metrics.sla_reply_time_calendar_hours
            ) as first_reply_time_sla,
            zendesk_ticket_metrics.sla_reply_time_calendar_hours
        from zendesk_ticket_metrics
        left join
            zendesk_ticket_emergency_sla_policy
            on zendesk_ticket_metrics.ticket_id
            = zendesk_ticket_emergency_sla_policy.ticket_id

    ),
    zendesk_ticket_sla_metric as (

        select
            zendesk_ticket_reply_time.*,
            timeadd(
                minute,
                zendesk_ticket_reply_time.sla_reply_time_calendar_hours,
                zendesk_ticket_reply_time.created_at
            ) as first_reply_at,
            -- Stitch does not send over timestamps of first replies, only duration in
            -- minutes
            zendesk_ticket_audit_sla.sla_policy,
            zendesk_ticket_audit_sla.sla_audit_created_at,
            zendesk_ticket_audit_sla.sla_audit_rank,
            zendesk_ticket_audit_sla.sla_audit_event_rank,
            zendesk_ticket_audit_priority.priority,
            zendesk_ticket_audit_priority.priority_audit_created_at,
            zendesk_ticket_audit_priority.priority_audit_rank,
            zendesk_ticket_audit_priority.priority_audit_event_rank
        from zendesk_ticket_reply_time
        left join
            zendesk_ticket_audit_sla
            on zendesk_ticket_reply_time.ticket_id = zendesk_ticket_audit_sla.ticket_id
        left join
            zendesk_ticket_audit_priority
            on zendesk_ticket_reply_time.ticket_id
            = zendesk_ticket_audit_priority.ticket_id
        where
            sla_audit_created_at <= first_reply_at
            and priority_audit_created_at <= first_reply_at

    ),
    zendesk_ticket_audit_minimum_ranks as (

        select
            ticket_id,
            min(sla_audit_rank) as min_sla_audit_rank,
            min(sla_audit_event_rank) as min_sla_audit_event_rank,
            min(priority_audit_rank) as min_priority_audit_rank,
            min(priority_audit_event_rank) as min_priority_audit_event_rank
        /* minimum rank (latest occurrence) of each SLA policy and priority assignment
        prior to or at the time of first reply */
        from zendesk_ticket_sla_metric
        group by 1

    ),
    final as (

        select distinct
            zendesk_ticket_sla_metric.ticket_id,
            zendesk_ticket_sla_metric.created_at,
            zendesk_ticket_sla_metric.priority,
            zendesk_ticket_sla_metric.sla_policy,
            zendesk_ticket_sla_metric.first_reply_time_sla,
            zendesk_ticket_sla_metric.first_reply_at
        from zendesk_ticket_sla_metric
        inner join
            zendesk_ticket_audit_minimum_ranks
            on zendesk_ticket_sla_metric.ticket_id
            = zendesk_ticket_audit_minimum_ranks.ticket_id
            and zendesk_ticket_sla_metric.sla_audit_rank
            = zendesk_ticket_audit_minimum_ranks.min_sla_audit_rank
            and zendesk_ticket_sla_metric.sla_audit_event_rank
            = zendesk_ticket_audit_minimum_ranks.min_sla_audit_event_rank
            and zendesk_ticket_sla_metric.priority_audit_rank
            = zendesk_ticket_audit_minimum_ranks.min_priority_audit_rank
            and zendesk_ticket_sla_metric.priority_audit_event_rank
            = zendesk_ticket_audit_minimum_ranks.min_priority_audit_event_rank

    )

select *
from final
