{{ config(tags=["mnpi"]) }}

with
    zuora_revenue_revenue_contract_hold as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_hold") }}
        qualify rank() OVER (partition by rc_hold_id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            rc_hold_id::varchar as revenue_contract_hold_id,
            rc_id::varchar as revenue_contract_id,
            rc_hold_applied_by::varchar as revenue_contract_hold_applied_by_id,
            rc_hold_applied_by_name::varchar as revenue_contract_hold_applied_by_name,
            concat(
                rc_hold_applied_prd_id::varchar, '01'
            ) as revenue_contract_hold_applied_period_id,
            rc_hold_applied_dt::datetime as revenue_contract_hold_applied_date,
            rc_hold_comment::varchar as revenue_contract_hold_comment,
            rc_hold_released_flag::varchar as is_revenue_contract_hold_released,
            rc_hold_release_comment::varchar as revenue_contract_hold_release_comment,
            rc_hold_release_reason::varchar as revenue_contract_hold_release_reason,
            concat(
                rc_hold_release_prd_id::varchar, '01'
            ) as revenue_contract_hold_release_period_id,
            rc_hold_release_dt::datetime as revenue_contract_hold_release_date,
            rc_hold_release_by::varchar as revenue_contract_hold_release_by_id,
            rc_hold_release_by_name::varchar as revenue_contract_hold_release_by_name,
            hold_id::varchar as hold_id,
            hold_type::varchar as hold_type,
            hold_name::varchar as hold_name,
            hold_desc::varchar as hold_description,
            rev_schd_hold_flag::varchar as is_revenue_schedule_hold,
            revrec_hold_flag::varchar as is_revenue_recognition_hold,
            alloc_schd_hold_flag::varchar as is_allocation_schedule_hold,
            alloc_rec_hold_flag::varchar as is_allocation_recognition_hold,
            user_releasable_flag::varchar as is_user_releasable,
            sec_atr_val::varchar as security_attribute_value,
            client_id::varchar as client_id,
            book_id::varchar as book_id,
            hold_crtd_by::varchar as hold_created_by,
            hold_crtd_dt::datetime as hold_created_date,
            hold_updt_by::varchar as hold_update_by,
            hold_updt_dt::datetime as hold_update_date,
            rc_hold_crtd_by::varchar as revenue_contract_hold_created_by,
            rc_hold_crtd_dt::datetime as revenue_contract_hold_created_date,
            rc_hold_updt_by::varchar as revenue_contract_hold_updated_by,
            rc_hold_updt_dt::datetime as revenue_contract_hold_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            rc_hold_exp_date::datetime as revenue_contract_hold_expiration_date,
            rc_hold_acct_segments::varchar as revenue_contract_hold_accounting_segment,
            allow_manual_apply_flag::varchar as is_allow_manual_apply,
            allow_manual_rel_flag::varchar as is_allow_manual_rel,
            hold_level::varchar as hold_level,
            hold_schd_type::varchar as hold_schedule_type,
            exp_fld_name::varchar as expiry_field_name,
            exp_num_type::varchar as expiry_number_type,
            exp_num::varchar as expiry_number,
            ln_hold_level_flag::varchar as line_hold_level,
            ln_hold_type_flag::varchar as line_hold_type,
            ln_hold_processed_flag::varchar as is_line_hold_processed,
            rev_hold_start_date::datetime as revenue_hold_start_date,
            rev_hold_end_date::datetime as revenue_hold_end_date,
            criteria_match_flag::varchar as is_criteria_match,
            remove_hold_flag::varchar as is_remove_hold,
            line_id::varchar as revenue_contract_line_id,
            event_id::varchar as event_id,
            manual_hold_flag::varchar as is_manual_hold,
            evnt_hold_appld_manul_flag::varchar as is_manual_event_hold_applied,
            override_aprv_flag::varchar as is_override_approval,
            sha_enabled_flag::varchar as is_sha_enabled,
            rc_pob_id::varchar as revenue_contract_performance_obligation_id

        from zuora_revenue_revenue_contract_hold

    )

select *
from renamed
