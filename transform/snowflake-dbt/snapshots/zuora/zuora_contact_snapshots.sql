{% snapshot zuora_contact_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}} select * from {{ source("zuora", "contact") }}

{% endsnapshot %}
