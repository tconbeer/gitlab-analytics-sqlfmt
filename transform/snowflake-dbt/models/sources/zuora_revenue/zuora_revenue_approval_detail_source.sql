with
    zuora_revenue_approval_detail as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_approval_detail") }}
        qualify
            rank() over (
                partition by rc_appr_id, approver_sequence, approval_rule_id
                order by incr_updt_dt desc
            )
            = 1

    ),
    renamed as (

        select

            {{
                dbt_utils.surrogate_key(
                    ["rc_appr_id", "approver_sequence", "approval_rule_id"]
                )
            }} as primary_key,
            rc_appr_id::varchar as revenue_contract_approval_id,
            rc_id::varchar as revenue_contract_id,
            approval_obj_type::varchar as approval_object_type,
            approval_status::varchar as approval_status,
            approver_comments::varchar as approver_comments,
            approved_by_user::varchar as approved_by_user_id,
            approved_by_name::varchar as approved_by_user_name,
            approval_date::datetime as approval_date,
            pending_since::datetime as pending_since,
            approver_user::varchar as approver_user_id,
            approver_name::varchar as approver_user_name,
            approver_sequence::varchar as approver_sequence,
            approval_rule_id::varchar as approver_rule_id,
            approval_rule_name::varchar as approval_rule_name,
            approval_type::varchar as approval_type,
            approval_rule_desc::varchar as approval_rule_description,
            book_id::varchar as book_id,
            sec_atr_val::varchar as security_attribute_value,
            client_id::varchar as client_id,
            rc_appr_crtd_by::varchar as revenue_contract_approval_created_by,
            rc_appr_crtd_dt::datetime as revenue_contract_approval_created_date,
            rc_appr_updt_by::varchar as revenue_contract_approval_updated_by,
            rc_appr_updt_dt::datetime as revenue_contract_approval_updated_date,
            rc_rule_crtd_by::varchar as revenue_contract_rule_created__by,
            rc_rule_crtd_dt::datetime as revenue_contract_rule_created_date,
            rc_rule_updt_by::varchar as revenue_contract_rule_updated_by,
            rc_rule_updt_dt::datetime as revenue_contract_rule_updatd_date,
            appr_rule_crtd_by::varchar as approval_rule_created_by,
            appr_rule_crtd_dt::datetime as approval_rule_created_date,
            appr_rule_updt_by::varchar as approval_rule_updated_by,
            appr_rule_updt_dt::datetime as approval_rule_updated_date,
            incr_updt_dt::datetime as incremental_update_date,
            approval_start_date::datetime as approval_start_date,
            approval_end_date::datetime as approval_end_date,
            rc_rule_id::varchar as revenue_contract_rule_id,
            rc_rule_rule_id::varchar as revenue_contract_rule_rule_id,
            rc_rule_obj_type::varchar as revenue_contract_rule_object_type,
            rev_schd_flag::varchar as is_revenue_schedule,
            override_approver_flag::varchar as is_override_approve,
            func_name::varchar as function_name,
            rule_id::varchar as rule_id,
            rc_rev_rel_appr_flag::varchar as is_revenue_contract_revenue_rel_approver,
            appr_removal_flag::varchar as is_approval_removal,
            rule_rev_rel_appr_flag::varchar as is_rule_revenue_approve,
            override_aprv_flag::varchar as is_override_aprv

        from zuora_revenue_approval_detail

    )

select *
from renamed
