{% snapshot zuora_product_rate_plan_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}}

select *
from {{ source("zuora", "product_rate_plan") }}

{% endsnapshot %}
