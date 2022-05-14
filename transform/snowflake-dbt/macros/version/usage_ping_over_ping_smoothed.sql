{%- macro usage_ping_over_ping_smoothed(
    all_time_event_metric_column,
    partition_by_columns=["dim_subscription_id", "uuid", "hostname"],
    order_by_column="snapshot_month",
    days_since_last_ping="days_since_last_ping",
    days_in_month="days_in_month_count"
) -%}

{%- set events_since_last_ping =  all_time_event_metric_column ~ '_since_last_ping' -%}
{%- set events_per_day_alias =  all_time_event_metric_column ~ '_estimated_daily' -%}
{%- set events_smoothed_alias =  all_time_event_metric_column ~ '_smoothed' -%}

{{ all_time_event_metric_column }},
{{ events_since_last_ping }},
first_value({{ events_since_last_ping }} / {{ days_since_last_ping }})
ignore nulls over (
    partition by
        {%- for column in partition_by_columns %}
        {{ column }} {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    order by {{ order_by_column }}
    rows between current row and unbounded following
) as {{ events_per_day_alias }},
({{ events_per_day_alias }} * {{ days_in_month }})::int as {{ events_smoothed_alias }}

{%- endmacro -%}
