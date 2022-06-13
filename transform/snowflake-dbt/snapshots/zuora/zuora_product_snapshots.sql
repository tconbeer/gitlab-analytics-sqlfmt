{% snapshot zuora_product_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}} select * from {{ source("zuora", "product") }}

{% endsnapshot %}
