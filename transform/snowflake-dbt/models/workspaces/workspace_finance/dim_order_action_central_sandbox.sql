with
    base as (

        select *
        from {{ ref("zuora_central_sandbox_order_action_source") }}
        where is_deleted = false

    ),
    final as (

        select

            order_action_id as dim_order_action_id,
            order_id as dim_order_id,
            subscription_id as dim_subscription_id,
            subscription_version_amendment_id as dim_amendment_id,
            type as order_action_type,
            sequence as order_action_sequence,
            auto_renew as is_auto_renew,
            cancellation_policy,
            term_type,
            created_date as order_action_created_date,
            customer_acceptance_date,
            contract_effective_date,
            service_activation_date,
            current_term,
            current_term_period_type,
            renewal_term,
            renewal_term_period_type,
            renew_setting as renewal_setting,
            term_start_date

        from base

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-03-31",
            updated_date="2022-03-31",
        )
    }}
