{% snapshot zuora_revenue_revenue_contract_hold_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="rc_hold_id",
        updated_at="incr_updt_dt",
    )
}}

select *
from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_hold") }}
qualify rank() over (partition by rc_hold_id order by incr_updt_dt desc) = 1

{% endsnapshot %}
