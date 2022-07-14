with
    zuora_revenue_revenue_contract_header as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_header") }}
        qualify rank() OVER (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            id::varchar as revenue_contract_id,
            version::varchar as revenue_contract_version,
            batch_id::varchar as revenue_contract_batch_id,
            book_id::varchar as book_id,
            list_amt::float as list_amount,
            sell_amt::float as sell_amount,
            ca_amt::float as contract_asset_amount,
            cl_amt::float as contract_liability_amount,
            tot_cv_amt::float as carve_amount,
            tmpl_id::varchar as template_id,
            fv_date::datetime as fair_value_date,
            curr::varchar as transactional_currency,
            f_cur::varchar as functional_currency,
            f_ex_rate::varchar as functional_currency_exchange_rate,
            g_ex_rate::varchar as reporting_currency_exchange_rate,
            atr1::varchar as revenue_contract_attribute_1,
            atr2::varchar as revenue_contract_attribute_2,
            atr3::varchar as revenue_contract_attribute_3,
            atr4::varchar as revenue_contract_attribute_4,
            atr5::varchar as revenue_contract_attribute_5,
            company_code::varchar as company_code,
            init_pob_exp_dt::datetime as initial_performance_obligation_expiration_date,
            cstmr_nm::varchar as customer_name,
            posted_flag::varchar as is_posted,
            alloc_error_flag::varchar as is_allocation_error,
            multi_curr_flag::varchar as is_multiple_currency,
            alloc_eligible_flag::varchar as is_allocation_eligible,
            manual_cv_flag::varchar as is_manual_cv,
            manual_rc_flag::varchar as is_manual_revenue_contract,
            freeze_flag::varchar as is_freeze,
            approval_status_flag::varchar as revnue_contract_approval_status,
            conversion_flag::varchar as is_conversion,
            archive_flag::varchar as is_archive,
            stale_group_flag::varchar as is_stale_group,
            initial_alloc_flag::varchar as is_initial_allocation,
            alloc_trtmt_flag::varchar as revenue_contract_allocation_treatment,
            schd_hold_flag::varchar as is_schedule_hold,
            revrev_hold_flag::varchar as is_revrev_hold,
            alloc_schd_hold_flag::varchar as is_allocation_schedule_hold,
            alloc_rec_hold_flag::varchar as is_allocation_recognition_hold,
            mje_rc_flag::varchar as is_manual_journal_entry_revenue_contract,
            netting_pending_flag::varchar as is_netting_pending,
            is_hold_applied_flag::varchar as is_hold_applied,
            concat(crtd_prd_id::varchar, '01') as revenue_contract_created_period_id,
            client_id::varchar as client_id,
            sec_atr_val::varchar as security_attribute_value,
            crtd_by::varchar as revenue_contract_created_by,
            crtd_dt::datetime as revenue_contract_created_date,
            updt_by::varchar as revenue_contract_updated_by,
            updt_dt::datetime as revenue_contract_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            ct_mod_end_dt::datetime as revenue_contract_modification_end_date,
            is_allocatable_flag::varchar as is_allocatable,
            inter_company_flag::varchar as is_inter_company,
            rc_crtd_manual_flag::varchar as is_revenue_contract_manually_created,
            rc_ssp_pct::varchar as revenue_contract_ssp_percent,
            multi_f_curr_flag::varchar as is_multi_functional_currency,
            skip_allocation_flag::varchar as is_skip_allocation,
            vc_allocation_flag::varchar as is_variable_consideration_allocation,
            rc_closed_flag::varchar as is_revenue_contract_closed,
            concat(timeline_period_flag::varchar, '01') as timeline_period,
            new_rc_by_ctmod_flag::varchar as is_new_revenue_contact_created_by_ct_mod,
            manual_hold_flag::varchar as is_manual_hold,
            acct_updt_dt::datetime as account_updated_date,
            multi_t_curr_flag::varchar as is_multiple_transactional_currency,
            retro_pros_flag::varchar as is_retro_pros,
            exception_flag::varchar as is_exception,
            rev_rel_appr_flag::varchar as is_rev_rel_approval,
            concat(max_schd_prd::varchar, '01') as max_schedule_period,
            lifecycle_change_dt::datetime as lifecycle_change_date,
            old_ct_mod_end_dt::datetime as old_revenue_contract_modification_end_date,
            lifecycle_state_flag::varchar as lifecycle_state,
            skip_ctmod_flag::varchar as is_skip_revenue_contract_modification,
            hybrid_rc_flag::varchar as is_hybrid_revenue_contract,
            get_delink_flag::varchar as is_delinked,
            ltst_mje_flag::varchar as is_lt_st_manual_journal_entry,
            alloc_multi_curr_flag::varchar as is_multiple_currency_allocation,
            alloc_multi_f_curr_flag::varchar
            as is_multiple_functional_currency_allocation,
            alloc_multi_t_curr_flag::varchar
            as is_multiple_transactional_currency_allocation

        from zuora_revenue_revenue_contract_header

    )

select *
from renamed
