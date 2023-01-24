with
    zuora_revenue_accounting_type as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_accounting_type") }}
        qualify rank() over (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            id::varchar as accounting_type_id,
            name::varchar as accounting_type_name,
            wf_type::varchar as waterfall_type,
            wf_summ_type::varchar as waterfall_summary_type,
            revenue_summary::varchar as revenue_summary_type,
            balance_sheet_acct_flag::varchar as is_balance_sheet_account,
            p_l_acct_flag::varchar as is_p_l_account,
            cost_flag::varchar as is_cost,
            vc_acct_flag::varchar as is_variable_consideration_account,
            vc_clr_acct_flag::varchar as is_variable_consideration_clearing_account,
            incl_in_netting_flag::varchar as is_include_in_netting,
            incl_in_manual_je_flag::varchar as is_include_in_manual_journal_entry,
            waterfall_flag::varchar as is_waterfall_account,
            acct_group::varchar as accounting_group,
            def_rec_flag::varchar as defer_recognition_type,
            client_id::varchar as client_id,
            crtd_by::varchar as accounting_type_created_by,
            crtd_dt::datetime as accounting_type_created_date,
            updt_by::varchar as accounting_type_updated_by,
            updt_dt::datetime as accounting_type_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            allow_mapping_flag::varchar as is_mapping_allowed,
            def_offset_flag::varchar as is_deferred_offset,
            enabled_flag::varchar as is_enabled,
            payables_acct_flag::varchar as is_payables_account,
            rev_offset_flag::varchar as is_revenue_offset,
            rev_display_seq::varchar as revenue_display_sequence

        from zuora_revenue_accounting_type

    )

select *
from renamed
