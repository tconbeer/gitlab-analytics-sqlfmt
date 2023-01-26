{% snapshot zuora_revenue_revenue_contract_schedule_deleted_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="schd_id",
        updated_at="incr_updt_dt",
    )
}}

select *
from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_schedule_deleted") }}
qualify rank() over (partition by schd_id order by incr_updt_dt desc) = 1

{% endsnapshot %}
