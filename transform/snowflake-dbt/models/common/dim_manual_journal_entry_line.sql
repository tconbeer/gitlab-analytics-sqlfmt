{{ config(tags=["mnpi_exception"]) }}

with
    mje_source as (

        select * from {{ ref("zuora_revenue_manual_journal_entry_source") }}

    ),
    final as (

        select

            -- ids 
            manual_journal_entry_line_id as dim_manual_journal_entry_line_id,

            -- line details
            manual_journal_entry_line_description,
            manual_journal_entry_line_comments,
            manual_journal_entry_line_type,
            manual_journal_entry_line_start_date,
            manual_journal_entry_line_end_date,
            set_of_books_name,
            currency,
            functional_currency,
            activity_type,
            reason_code,
            error_message,
            approver_name,
            approval_status,
            reversal_status,
            reversal_approval_status,
            revenue_recognition_type,
            debit_activity_type,
            credit_activity_type,
            date_format,

            -- metadata
            manual_journal_entry_line_created_by,
            manual_journal_entry_line_created_date,
            manual_journal_entry_line_updated_by,
            manual_journal_entry_line_updated_date,
            security_attribute_value

        from mje_source

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
