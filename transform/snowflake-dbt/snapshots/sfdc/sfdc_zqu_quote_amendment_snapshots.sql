{% snapshot sfdc_zqu_quote_amendment_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="systemmodstamp",
    )
}}

select *
from {{ source("salesforce", "zqu_quote_amendment") }}

{% endsnapshot %}
