{% snapshot sfdc_user_snapshots %}

    {{
        config(
            unique_key="id",
            strategy="timestamp",
            updated_at="systemmodstamp",
        )
    }}

    select *
    from {{ source("salesforce", "user") }}

{% endsnapshot %}
