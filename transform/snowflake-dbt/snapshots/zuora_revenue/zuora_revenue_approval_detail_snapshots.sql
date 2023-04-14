{% snapshot zuora_revenue_approval_detail_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="rc_id",
            updated_at="incr_updt_dt",
        )
    }}

    select *
    from {{ source("zuora_revenue", "zuora_revenue_approval_detail") }}
    qualify
        rank() over (
            partition by rc_appr_id, approver_sequence, approval_rule_id
            order by incr_updt_dt desc
        )
        = 1

{% endsnapshot %}
