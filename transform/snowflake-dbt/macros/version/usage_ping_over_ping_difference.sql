{%- macro usage_ping_over_ping_difference(
    all_time_event_metric_column,
    partition_by_columns=["dim_subscription_id", "uuid", "hostname"],
    order_by_column="snapshot_month"
) -%}

    {%- set ping_over_ping_alias = (
        all_time_event_metric_column ~ "_since_last_ping"
    ) -%}

    {{ all_time_event_metric_column }},
    {{ all_time_event_metric_column }} - lag({{ all_time_event_metric_column }})
    ignore nulls over (
        partition by
            {%- for column in partition_by_columns %}
                {{ column }} {%- if not loop.last -%},{% endif %}
            {%- endfor %}
        order by {{ order_by_column }}
    ) as {{ ping_over_ping_alias }}

{%- endmacro -%}
