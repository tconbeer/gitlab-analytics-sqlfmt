with
    hold_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_hold_source") }}

    ),
    final as (

        select

            -- ids
            revenue_contract_hold_id as dim_revenue_contract_hold_id,
            revenue_contract_id as dim_revenue_contract_id,
            revenue_contract_line_id as dim_revenue_contract_line_id,
            revenue_contract_performance_obligation_id
            as dim_revenue_contract_performance_obligation_id,
            event_id as dim_accounting_event_id,

            -- accounting segment
            revenue_contract_hold_accounting_segment,

            -- dates
            {{ get_date_id("revenue_hold_start_date") }} as revenue_hold_start_date_id,
            {{ get_date_id("revenue_hold_end_date") }} as revenue_hold_end_date_id,
            {{ get_date_id("revenue_contract_hold_release_date") }}
            as revenue_contract_hold_release_date_id,
            {{ get_date_id("revenue_contract_hold_expiration_date") }}
            as revenue_contract_hold_expiration_date_id,

            -- flags
            is_revenue_schedule_hold,
            is_revenue_recognition_hold,
            is_allocation_schedule_hold,
            is_allocation_recognition_hold,
            is_user_releasable,
            is_criteria_match,
            is_remove_hold,
            is_manual_hold,
            is_manual_event_hold_applied,
            is_override_approval,
            is_sha_enabled,
            is_allow_manual_apply,
            is_allow_manual_rel,
            is_line_hold_processed,

            -- metadata
            revenue_contract_hold_created_by,
            {{ get_date_id("revenue_contract_hold_created_date") }}
            as revenue_contract_hold_created_date_id,
            revenue_contract_hold_updated_by,
            {{ get_date_id("revenue_contract_hold_updated_date") }}
            as revenue_contract_hold_updated_date_id,
            {{ get_date_id("incremental_update_date") }} as incremental_update_date_id,
            security_attribute_value

        from hold_source

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2021-06-21",
            updated_date="2021-06-21",
        )
    }}
