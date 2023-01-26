with
    schedule_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_schedule_source") }}

    ),
    final as (

        select

            -- ids
            revenue_contract_schedule_id as dim_revenue_contract_schedule_id,
            revenue_contract_id as dim_revenue_contract_id,
            revenue_contract_line_id as dim_revenue_contract_line_id,
            original_revenue_contract_line_id as dim_revenue_contract_line_id_original,
            performance_obligation_id as dim_performance_obligation_id,
            reference_revenue_contract_bill_id as dim_invoice_id,
            manual_journal_entry_header_id as dim_manual_journal_entry_header_id,
            post_batch_id,
            release_action_id,
            accounting_type_id as dim_accounting_type_id,
            debit_link_id,
            credit_link_id,

            -- dates
            {{ get_date_id("post_date") }} as post_date_id,
            {{ get_date_id("period_id") }} as period_date_id,
            {{ get_date_id("posted_period_id") }} as posted_period_date_id,
            {{ get_date_id("billed_fx_date") }} as billed_fx_date_id,

            -- additive fields
            amount,
            transactional_debit_amount,
            transactional_credit_amount,
            functional_debit_amount,
            functional_credit_amount,
            reporting_debit_amount,
            reporting_credit_amount,
            previous_period_amount,
            previous_quarter_amount,
            previous_year_amount,
            release_percent,

            -- exchange rates
            exchange_rate_date,
            functional_currency_exchange_rate,
            reporting_currency_exchange_rate,
            billed_fx_rate,

            -- segments
            accounting_segment,
            credit_accounting,
            debit_accounting,

            -- flags
            is_interfaced,
            is_initial_entry,
            is_reversal,
            is_forecast,
            is_previous_period_contract_liability,
            is_netting_entry,
            is_reallocation,
            is_initial_reporting_entry,
            is_impact_transaction_price,
            is_unbilled,
            is_mass_action,
            is_special_allocation,
            is_previous_period_adjustment,
            is_variable_consideration_expiry_schedule,
            is_previous_quarter_adjustment,
            is_delink,
            is_retro_reversal,
            is_pord,
            is_unbilled_reversal,
            is_recognition_event_account,
            is_cmro_contra_entry,
            is_retro_entry,
            is_left_over_entry,
            is_revs_posted_invoice,
            is_contract_liability_dist_entry,

            -- metadata
            revenue_contract_schedule_created_by,
            {{ get_date_id("revenue_contract_schedule_created_date") }}
            as revenue_contract_schedule_created_date_id,
            revenue_contract_schedule_updated_by,
            {{ get_date_id("revenue_contract_schedule_updated_date") }}
            as revenue_contract_schedule_updated_date_id,
            {{ get_date_id("incremental_update_date") }} as incremental_update_date_id,
            security_attribute_value

        from schedule_source

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
