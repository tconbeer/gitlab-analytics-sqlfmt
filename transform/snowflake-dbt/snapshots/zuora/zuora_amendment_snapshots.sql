{% snapshot zuora_amendment_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}}

select *
from {{ source("zuora", "amendment") }}

{% endsnapshot %}
