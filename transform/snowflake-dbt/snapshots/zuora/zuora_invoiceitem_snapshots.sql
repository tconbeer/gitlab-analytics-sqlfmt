{% snapshot zuora_invoiceitem_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}} select * from {{ source("zuora", "invoice_item") }}

{% endsnapshot %}
