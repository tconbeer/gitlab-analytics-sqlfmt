{% snapshot zuora_revenue_revenue_contract_schedule_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="revenue_snapshot_id",
            updated_at="incr_updt_dt",
        )
    }}

    select
        rc_id
        || '-'
        || crtd_prd_id
        || '-'
        || root_line_id
        || '-'
        || ref_bill_id
        || '-'
        || schd_id
        || '-'
        || line_id
        || '-'
        || acctg_seg as revenue_snapshot_id,
        *
    from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_schedule") }}
    qualify
        rank() over (partition by schd_id, acctg_type order by incr_updt_dt desc) = 1

{% endsnapshot %}
