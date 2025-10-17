with
    accounting_type_source as (

        select * from {{ ref("zuora_revenue_accounting_type_source") }}

    ),
    final as (

        select

            -- ids
            accounting_type_id as dim_accounting_type_id,

            -- accounting type attributes
            accounting_type_name,
            waterfall_type,
            waterfall_summary_type,
            revenue_summary_type,
            accounting_group,
            defer_recognition_type,
            revenue_display_sequence,

            -- flags
            is_balance_sheet_account,
            is_p_l_account,
            is_cost,
            is_variable_consideration_account,
            is_variable_consideration_clearing_account,
            is_include_in_netting,
            is_include_in_manual_journal_entry,
            is_waterfall_account,
            is_mapping_allowed,
            is_deferred_offset,
            is_enabled,
            is_payables_account,
            is_revenue_offset,

            -- metadata
            accounting_type_created_by,
            accounting_type_created_date,
            accounting_type_updated_by,
            accounting_type_updated_date,
            incremental_update_date

        from accounting_type_source

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
