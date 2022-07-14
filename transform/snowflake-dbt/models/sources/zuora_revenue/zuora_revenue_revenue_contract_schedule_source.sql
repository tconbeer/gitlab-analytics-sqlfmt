{{ config(tags=["mnpi"]) }}

with
    zuora_revenue_revenue_contract_schedule as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_schedule") }}
        qualify
            rank() OVER (partition by schd_id, acctg_type order by incr_updt_dt desc)
            = 1

    ),
    renamed as (

        select

            {{ dbt_utils.surrogate_key(["schd_id", "acctg_type"]) }} as primary_key,
            rc_id::varchar as revenue_contract_id,
            rc_ver::varchar as revenue_contract_version,
            dist_id::varchar as dist_id,
            atr1::varchar as revenue_contract_schedule_attribute_1,
            atr2::varchar as revenue_contract_schedule_attribute_2,
            atr3::varchar as revenue_contract_schedule_attribute_3,
            atr4::varchar as revenue_contract_schedule_attribute_4,
            atr5::varchar as revenue_contract_schedule_attribute_5,
            pob_id::varchar as performance_obligation_id,
            amount::float as amount,
            curr::varchar as transactional_currency,
            f_ex_rate::float as functional_currency_exchange_rate,
            ex_rate_date::datetime as exchange_rate_date,
            post_date::datetime as post_date,
            concat(prd_id, '01')::varchar as period_id,
            post_prd_id::varchar as posted_period_id,
            post_batch_id::varchar as post_batch_id,
            g_ex_rate::float as reporting_currency_exchange_rate,
            rel_id::varchar as release_action_id,
            rel_pct::float as release_percent,
            concat(crtd_prd_id, '01')::varchar
            as revenue_contract_schedule_created_period_id,
            root_line_id::varchar as root_line_id,
            ref_bill_id::varchar as reference_revenue_contract_bill_id,
            schd_id::varchar as revenue_contract_schedule_id,
            line_id::varchar as revenue_contract_line_id,
            acctg_seg::varchar as accounting_segment,
            dr_amount::float as transactional_debit_amount,
            cr_amount::float as transactional_credit_amount,
            f_dr_amount::float as functional_debit_amount,
            f_cr_amount::float as functional_credit_amount,
            g_dr_amount::float as reporting_debit_amount,
            g_cr_amount::float as reporting_credit_amount,
            acctg_type::varchar as accounting_type_id,
            interfaced_flag::varchar as is_interfaced,
            initial_entry_flag::varchar as is_initial_entry,
            reversal_flag::varchar as is_reversal,
            fcst_flag::varchar as is_forecast,
            pp_cl_flag::varchar as is_previous_period_contract_liability,
            netting_entry_flag::varchar as is_netting_entry,
            reallocation_flag::varchar as is_reallocation,
            account_name::varchar as account_name,
            schd_type_flag::varchar as schedule_type,
            initial_rep_entry_flag::varchar as is_initial_reporting_entry,
            period_name::varchar as period_name,
            client_id::varchar as client_id,
            book_id::varchar as book_id,
            sec_atr_val::varchar as security_attribute_value,
            crtd_by::varchar as revenue_contract_schedule_created_by,
            crtd_dt::datetime as revenue_contract_schedule_created_date,
            updt_by::varchar as revenue_contract_schedule_updated_by,
            updt_dt::datetime as revenue_contract_schedule_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            impact_trans_prc_flag::varchar as is_impact_transaction_price,
            line_type_flag::varchar as revenue_contract_line_type,
            unbilled_flag::varchar as is_unbilled,
            bld_fx_dt::datetime as billed_fx_date,
            bld_fx_rate::float as billed_fx_rate,
            rord_inv_ref::varchar as reduction_order_invoice_reference,
            cr_acctg_flag::varchar as credit_accounting,
            dr_acctg_flag::varchar as debit_accounting,
            mass_action_flag::varchar as is_mass_action,
            special_alloc_flag::varchar as is_special_allocation,
            pp_adj_flag::varchar as is_previous_period_adjustment,
            vc_expiry_schd_flag::varchar as is_variable_consideration_expiry_schedule,
            orig_line_id::varchar as original_revenue_contract_line_id,
            dr_link_id::varchar as debit_link_id,
            cr_link_id::varchar as credit_link_id,
            model_id::varchar as model_id,
            je_batch_id::varchar as manual_journal_entry_header_id,
            je_batch_name::varchar as manual_journal_entry_header_name,
            pq_adj_flag::varchar as is_previous_quarter_adjustment,
            delink_flag::varchar as is_delink,
            retro_rvrsl_flag::varchar as is_retro_reversal,
            pp_amt::float as previous_period_amount,
            pq_amt::float as previous_quarter_amount,
            py_amt::float as previous_year_amount,
            updt_prd_id::varchar as revenue_contract_schedule_update_period_id,
            pord_flag::varchar as is_pord,
            unbill_rvrsl_flag::varchar as is_unbilled_reversal,
            rec_evt_act_flag::varchar as is_recognition_event_account,
            cmro_contra_entry_flag::varchar as is_cmro_contra_entry,
            retro_entry_flag::varchar as is_retro_entry,
            left_over_entry_flag::varchar as is_left_over_entry,
            revs_posted_inv_flag::varchar as is_revs_posted_invoice,
            cl_dist_entry_flag::varchar as is_contract_liability_dist_entry

        from zuora_revenue_revenue_contract_schedule

    )

select *
from renamed
