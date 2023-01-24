{% snapshot zuora_account_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}}

select *
from {{ source("zuora", "account") }}

{% endsnapshot %}
