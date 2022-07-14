{% snapshot zuora_revenue_manual_journal_entry_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="je_line_id",
        updated_at="incr_updt_dt",
    )
}}

select *
from {{ source("zuora_revenue", "zuora_revenue_manual_journal_entry") }}
qualify rank() OVER (partition by je_line_id order by incr_updt_dt desc) = 1

{% endsnapshot %}
