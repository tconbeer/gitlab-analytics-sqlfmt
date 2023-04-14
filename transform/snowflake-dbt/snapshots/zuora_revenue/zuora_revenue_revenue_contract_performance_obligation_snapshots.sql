{% snapshot zuora_revenue_revenue_contract_performance_obligation_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="rc_pob_id",
            updated_at="incr_updt_dt",
        )
    }}

    select *
    from
        {{
            source(
                "zuora_revenue",
                "zuora_revenue_revenue_contract_performance_obligation",
            )
        }}
    qualify rank() over (partition by rc_pob_id order by incr_updt_dt desc) = 1

{% endsnapshot %}
