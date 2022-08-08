{% snapshot zuora_revenue_invoice_accounting_summary_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="revenue_snapshot_id",
        updated_at="incr_updt_dt",
    )
}}

select
    prd_id
    || '-'
    || line_id
    || '-'
    || root_line_id
    || '-'
    || rc_id
    || '-'
    || acct_type_id
    || '-'
    || acctg_segs
    || '-'
    || schd_type as revenue_snapshot_id,
    *
from {{ source("zuora_revenue", "zuora_revenue_invoice_accounting_summary") }}
qualify rank() over (partition by revenue_snapshot_id order by updt_dt desc) = 1

{% endsnapshot %}
