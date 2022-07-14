{{ config(tags=["mnpi"]) }}

with
    zuora_revenue_manual_journal_entry as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_manual_journal_entry") }}
        qualify rank() OVER (partition by je_line_id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            je_head_id::varchar as manual_journal_entry_header_id,
            je_head_name::varchar as manual_journal_entry_header_name,
            je_head_desc::varchar as manual_journal_entry_header_description,
            je_head_cat_code::varchar as manual_journal_entry_header_category_code,
            je_head_ex_rate_type::varchar
            as manual_journal_entry_header_exchange_rate_type,
            hash_total::varchar as hash_total,
            sob_id::varchar as set_of_books_id,
            sob_name::varchar as set_of_books_name,
            fn_cur::varchar as functional_currency,
            -- Data received from Zuora in YYYYMM format, formatted to YYYYMMDD in the
            -- below.
            concat(rvsl_prd_id::varchar, '01') as reversal_period_id,
            concat(prd_id::varchar, '01') as period_id,
            je_head_atr1::varchar as manual_journal_entry_header_attribute_1,
            je_head_atr2::varchar as manual_journal_entry_header_attribute_2,
            je_head_atr3::varchar as manual_journal_entry_header_attribute_3,
            je_head_atr4::varchar as manual_journal_entry_header_attribute_4,
            je_head_atr5::varchar as manual_journal_entry_header_attribute_5,
            concat(
                je_head_crtd_prd_id::varchar, '01'
            ) as manual_journal_entry_header_created_period_id,
            je_line_id::varchar as manual_journal_entry_line_id,
            activity_type::varchar as activity_type,
            curr::varchar as currency,
            dr_cc_id::varchar as debit_account_code_combination_id,
            cr_cc_id::varchar as credit_account_code_combination_id,
            ex_rate_date::datetime as exchange_rate_date,
            ex_rate::varchar as exchange_rate,
            g_ex_rate::varchar as reporting_currency_exchange_rate,
            amount::float as amount,
            func_amount::float as funcional_currency_amount,
            start_date::datetime as manual_journal_entry_line_start_date,
            end_date::datetime as manual_journal_entry_line_end_date,
            reason_code::varchar as reason_code,
            description::varchar as manual_journal_entry_line_description,
            comments::varchar as manual_journal_entry_line_comments,
            dr_segment1::varchar as debit_segment_1,
            dr_segment2::varchar as debit_segment_2,
            dr_segment3::varchar as debit_segment_3,
            dr_segment4::varchar as debit_segment_4,
            dr_segment5::varchar as debit_segment_5,
            dr_segment6::varchar as debit_segment_6,
            dr_segment7::varchar as debit_segment_7,
            dr_segment8::varchar as debit_segment_8,
            dr_segment9::varchar as debit_segment_9,
            dr_segment10::varchar as debit_segment_10,
            cr_segment1::varchar as credit_segment_1,
            cr_segment2::varchar as credit_segment_2,
            cr_segment3::varchar as credit_segment_3,
            cr_segment4::varchar as credit_segment_4,
            cr_segment5::varchar as credit_segment_5,
            cr_segment6::varchar as credit_segment_6,
            cr_segment7::varchar as credit_segment_7,
            cr_segment8::varchar as credit_segment_8,
            cr_segment9::varchar as credit_segment_9,
            cr_segment10::varchar as credit_segment_10,
            reference1::varchar as manual_journal_entry_reference_1,
            reference2::varchar as manual_journal_entry_reference_2,
            reference3::varchar as manual_journal_entry_reference_3,
            reference4::varchar as manual_journal_entry_reference_4,
            reference5::varchar as manual_journal_entry_reference_5,
            reference6::varchar as manual_journal_entry_reference_6,
            reference7::varchar as manual_journal_entry_reference_7,
            reference8::varchar as manual_journal_entry_reference_8,
            reference9::varchar as manual_journal_entry_reference_9,
            reference10::varchar as manual_journal_entry_reference_10,
            reference11::varchar as manual_journal_entry_reference_11,
            reference12::varchar as manual_journal_entry_reference_12,
            reference13::varchar as manual_journal_entry_reference_13,
            reference14::varchar as manual_journal_entry_reference_14,
            reference15::varchar as manual_journal_entry_reference_15,
            sec_atr_val::varchar as security_attribute_value,
            book_id::varchar as book_id,
            client_id::varchar as client_id,
            je_head_crtd_by::varchar as manual_journal_entry_header_created_by,
            je_head_crtd_dt::datetime as manual_journal_entry_header_created_date,
            je_head_updt_by::varchar as manual_journal_entry_header_updated_by,
            je_head_updt_dt::datetime as manual_journal_entry_header_updated_date,
            je_line_crtd_by::varchar as manual_journal_entry_line_created_by,
            je_line_crtd_dt::datetime as manual_journal_entry_line_created_date,
            je_line_updt_by::varchar as manual_journal_entry_line_updated_by,
            je_line_updt_dt::datetime as manual_journal_entry_line_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            je_status_flag::varchar as manual_journal_entry_header_status,
            rev_rec_type_flag::varchar as is_revenue_recognition_type,
            je_type_flag::varchar as manual_journal_entry_header_type,
            summary_flag::varchar as is_summary,
            manual_reversal_flag::varchar as is_manual_reversal,
            reversal_status_flag::varchar as reversal_status,
            approval_status_flag::varchar as approval_status,
            reversal_approval_status_flag::varchar as reversal_approval_status,
            rev_rec_type::varchar as revenue_recognition_type,
            error_msg::varchar as error_message,
            dr_activity_type::varchar as debit_activity_type,
            cr_activity_type::varchar as credit_activity_type,
            active_flag::varchar as is_active,
            appr_name::varchar as approver_name,
            rc_id::varchar as revenue_contract_id,
            doc_line_id::varchar as doc_line_id,
            rc_line_id::varchar as revenue_contract_line_id,
            cst_or_vc_type::varchar as is_cost_or_vairable_consideration,
            type_name::varchar as manual_journal_entry_line_type,
            dt_frmt::varchar as date_format,
            opn_int_flag::varchar as is_open_interface,
            auto_appr_flag::varchar as is_auto_approved,
            unbilled_flag::varchar as is_unbilled

        from zuora_revenue_manual_journal_entry


    )

select *
from renamed
