{% snapshot netsuite_accounting_periods_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="id",
            updated_at="date_last_modified",
        )
    }}

    select concat(fiscal_calendar_id, accounting_period_id) as id, *
    from {{ source("netsuite", "accounting_periods") }}

{% endsnapshot %}
