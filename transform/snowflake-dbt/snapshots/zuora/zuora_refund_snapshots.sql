{% snapshot zuora_refund_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="id",
            updated_at="updateddate",
        )
    }}

    select *
    from {{ source("zuora", "refund") }}

{% endsnapshot %}
