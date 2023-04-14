{% snapshot netsuite_transaction_lines_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="unique_key",
            updated_at="date_last_modified_gmt",
        )
    }}

    select *
    from {{ source("netsuite", "transaction_lines") }}

{% endsnapshot %}
