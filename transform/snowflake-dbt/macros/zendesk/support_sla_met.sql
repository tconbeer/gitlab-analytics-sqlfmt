{% macro support_sla_met(first_reply_time, ticket_priority, created_at) %}

    {% set day_after_created = dbt_utils.dateadd(
        datepart="day", interval=1, from_date_or_timestamp=created_at
    ) %}
    {% set minutes_before_day_end = dbt_utils.datediff(
        created_at, day_after_created, "mins"
    ) %}

    case
        -- Logic for urgent tickets with a 24/7 SLA of 30 minutes
        when {{ first_reply_time }} <= 30 and {{ ticket_priority }} = 'urgent'
        then true
        -- Logic for high priority tickets with a 24/5 SLA of 4 hours or 240 minutes
        when {{ first_reply_time }} <= 240 and {{ ticket_priority }} = 'high'
        then true
        -- Logic for tickets submitted on a Friday after 8 pm.
        -- Minutes remaining in day + minutes elapsed over the weekend (2880) + unused
        -- SLA minutes
        when
            {{ ticket_priority }} = 'high'
            and dayofweek({{ created_at }}) = 5
            and hour({{ created_at }}) >= 20
            and {{ first_reply_time }}
            <= {{ minutes_before_day_end }}
            + 2880
            + (240 - {{ minutes_before_day_end }})
        then true
        -- Logic for high priority tickets submitted over the weekend. Minutes elapsed
        -- over the weekend (2880) + allotted SLA minutes
        when
            {{ ticket_priority }} = 'high'
            and dayofweek({{ created_at }}) = 6
            or dayofweek({{ created_at }}) = 0
            and {{ first_reply_time }} <= 2880 + 240
        then true
        -- Logic for normal priority tickets with a 24/5 SLA of 8 hours or 480 minutes
        when {{ first_reply_time }} <= 480 and {{ ticket_priority }} = 'normal'
        then true
        -- Logic for normal priority tickets submitted on a Friday after 4 pm. Minutes
        -- remaining in day +  minutes elapsed over the weekend (2880) + Unused SLA
        -- minutes
        when
            {{ ticket_priority }} = 'normal'
            and dayofweek({{ created_at }}) = 5
            and hour({{ created_at }}) >= 16
            and {{ first_reply_time }}
            <= {{ minutes_before_day_end }}
            + 2880
            + (480 - {{ minutes_before_day_end }})
        then true
        -- Logic for normal priority tickets submitted over the weekend. Minutes
        -- elapsed over the weekend + allotted SLA minutes
        when
            {{ ticket_priority }} = 'normal'
            and dayofweek({{ created_at }}) = 6
            or dayofweek({{ created_at }}) = 0
            and {{ first_reply_time }} <= 2880 + 480
        then true
        -- Logic for low priority tickets with a 24/5 SLA of 24 hours or 1440 minutes
        when {{ first_reply_time }} <= 1440 and {{ ticket_priority }} = 'low'
        then true
        -- Logic for low priority tickets submitted on a Friday after 12am. Minutes
        -- remaining in day + minutes elapsed over the weekend (2880) + unused SLA
        -- minutes
        when
            {{ ticket_priority }} = 'low'
            and dayofweek({{ created_at }}) = 5
            and hour({{ created_at }}) > 0
            and {{ first_reply_time }}
            <= {{ minutes_before_day_end }}
            + 2880
            + (1440 - {{ minutes_before_day_end }})
        then true
        -- Logic for low priority tickets submitted over the weekend. Minutes elapsed
        -- over the weekend (2880) + allotted SLA minutes
        when
            {{ ticket_priority }} = 'low'
            and dayofweek({{ created_at }}) = 6
            or dayofweek({{ created_at }}) = 0
            and {{ first_reply_time }} <= 2880 + 1440
        then true

        else false

    end

{% endmacro %}
