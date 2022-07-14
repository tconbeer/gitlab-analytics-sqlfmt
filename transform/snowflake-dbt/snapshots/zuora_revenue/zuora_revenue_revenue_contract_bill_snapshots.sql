{% snapshot zuora_revenue_revenue_contract_bill_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="id",
        updated_at="incr_updt_dt",
    )
}}

select *
from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_bill") }}
qualify rank() OVER (partition by id order by incr_updt_dt desc) = 1

{% endsnapshot %}
