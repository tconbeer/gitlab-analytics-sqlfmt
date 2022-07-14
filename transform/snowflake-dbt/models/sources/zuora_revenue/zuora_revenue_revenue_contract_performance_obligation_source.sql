{{ config(tags=["mnpi"]) }}

with
    zuora_revenue_revenue_contract_performance_obligation as (

        select *
        from
            {{
                source(
                    "zuora_revenue",
                    "zuora_revenue_revenue_contract_performance_obligation",
                )
            }}
        qualify rank() OVER (partition by rc_pob_id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            rc_pob_id::varchar as revenue_contract_performance_obligation_id,
            rc_id::varchar as revenue_contract_id,
            lead_line_id::varchar as lead_line_id,
            pob_version::varchar as performance_obligation_version,
            rc_pob_name::varchar as revenue_contract_performance_obligation_name,
            pob_dependency_flag::varchar as is_performance_obligation_dependency,
            pob_processed_flag::varchar as is_performance_obligation_processed,
            pob_removed_flag::varchar as is_performance_obligation_removed,
            pob_multi_sign_flag::varchar as is_performance_obligation_multiple_sign,
            pob_removal_flag::varchar as is_performance_obligation_removal,
            pob_manual_flag::varchar as is_performance_obligation_manual,
            pob_orphan_flag::varchar as is_performance_obligation_orphan,
            pob_manual_forecast_flag::varchar
            as is_performance_obligation_manual_forecast,
            pob_tmpl_id::varchar as performance_obligation_template_id,
            pob_tmpl_name::varchar as performance_obligation_template_name,
            pob_tmpl_desc::varchar as performance_obligation_template_description,
            pob_tmpl_version::varchar as performance_obligation_template_version,
            rev_rec_type::varchar as revenue_recognition_type,
            start_date::datetime as revenue_contract_performance_obligation_start_date,
            end_date::datetime as revenue_contract_performance_obligation_end_date,
            rev_timing::varchar as revenue_timing,
            rev_start_dt::datetime as revenue_start_date,
            rev_end_dt::datetime as revenue_end_date,
            duration::varchar as revenue_amortization_duration,
            rev_segments::varchar as revenue_accounting_segment,
            cv_in_segments::varchar as carve_in_accounting_segment,
            cv_out_segments::varchar as carve_out_accounting_segment,
            cl_segments::varchar as contract_liability_accounting_segment,
            ca_segments::varchar as contract_asset_accounting_segment,
            qty_distinct_flag::varchar as is_quantity_distinct,
            term_distinct_flag::varchar as is_term_distinct,
            apply_manually_flag::varchar as is_apply_manually,
            release_manually_flag::varchar as is_release_manually,
            rev_leading_flag::varchar as is_revenue_leading,
            cv_in_leading_flag::varchar as is_carve_in_leading,
            cv_out_leading_flag::varchar as is_carve_out_leading,
            cl_leading_flag::varchar as is_contract_liability_leading,
            ca_leading_flag::varchar as is_contract_asset_leading,
            rel_action_type_flag::varchar as release_action_type,
            pob_tmpl_dependency_flag::varchar
            as is_performance_obligation_template_dependency,
            latest_version_flag::varchar as is_latest_version,
            consumption_flag::varchar as is_consumption,
            pob_satisfied_flag::varchar as is_performance_obligation_satisfied,
            event_id::varchar as event_id,
            event_name::varchar as event_name,
            postable_flag::varchar as is_postable,
            event_type_flag::varchar as event_type,
            book_id::varchar as book_id,
            sec_atr_val::varchar as security_attribute_value,
            client_id::varchar as client_id,
            concat(
                crtd_prd_id::varchar, '01'
            ) as revenue_contract_performance_obligation_created_period_id,
            rc_pob_crtd_by::varchar
            as revenue_contract_performance_obligation_created_by,
            rc_pob_crtd_dt::datetime
            as revenue_contract_performance_obligation_created_date,
            rc_pob_updt_by::varchar
            as revenue_contract_performance_obligation_updated_by,
            rc_pob_updt_dt::datetime
            as revenue_contract_performance_obligation_updated_date,
            pob_tmpl_crtd_by::varchar as performance_obligation_template_created_by,
            pob_tmpl_crtd_dt::datetime as performance_obligation_template_created_date,
            pob_tmpl_updt_by::varchar as performance_obligation_template_updated_by,
            pob_tmpl_updt_dt::datetime as performance_obligation_template_updated_date,
            event_crtd_by::varchar as event_created_by,
            event_crtd_dt::datetime as event_created_date,
            event_updt_by::varchar as event_updated_by,
            event_updt_dt::datetime as event_updated_date,
            incr_updt_dt::varchar as incremental_update_date,
            pob_id::varchar as performance_obligation_id,
            natural_acct::varchar as natural_account,
            distinct_flag::varchar as is_distinct,
            tolerance_pct::float as tolerance_percent,
            evt_type_id::varchar as event_type_id,
            cmltv_prcs_flag::varchar as is_cumulative_prcs,
            exp_date::datetime as expiry_date,
            manual_edit_flag::varchar as is_manual_edit,
            rule_identifier::varchar as rule_identifier,
            exp_fld_name::varchar as expiry_field_name,
            exp_num::varchar as expiry_number,
            exp_num_type::varchar as expiry_number_type,
            rel_immediate_flag::varchar as is_release_immediate,
            so_term_change_flag::varchar as is_sales_order_term_change,
            event_column1::varchar as event_column_1,
            event_column2::varchar as event_column_2,
            event_column3::varchar as event_column_3,
            event_column4::varchar as event_column_4,
            event_column5::varchar as event_column_5,
            source_column1::varchar as source_colum_n1,
            source_column2::varchar as source_column_2,
            source_column3::varchar as source_column_3,
            source_column4::varchar as source_column_4,
            source_column5::varchar as source_column_5,
            order_column1::varchar as order_column_1,
            order_column2::varchar as order_column_2,
            order_column3::varchar as order_column_3,
            order_column4::varchar as order_column_4,
            order_column5::varchar as order_column_5,
            process_type::varchar as process_type,
            rel_base_date::datetime as release_base_date,
            retain_method::varchar as retain_method,
            manual_rearranged_flag::varchar as is_manual_rearranged,
            manual_release_flag::varchar as is_manual_release,
            fcst_tmpl_id::varchar as forecast_template_id

        from zuora_revenue_revenue_contract_performance_obligation

    )

select *
from renamed
