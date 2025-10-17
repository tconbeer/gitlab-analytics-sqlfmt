{% snapshot sfdc_zqu_quote_snapshots %}

    {{
        config(
            unique_key="id",
            strategy="timestamp",
            updated_at="systemmodstamp",
        )
    }}

    select *
    from {{ source("salesforce", "zqu_quote") }}

{% endsnapshot %}
