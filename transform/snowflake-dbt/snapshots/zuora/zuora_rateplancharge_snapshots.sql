{% snapshot zuora_rateplancharge_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="updateddate",
    )
}} select * from {{ source("zuora", "rate_plan_charge") }}

{% endsnapshot %}
