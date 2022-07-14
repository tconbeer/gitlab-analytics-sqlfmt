with
    schedule_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_schedule_source") }}

    ),
    calendar_source as (select * from {{ ref("zuora_revenue_calendar_source") }}),
    accounting_type_source as (

        select * from {{ ref("zuora_revenue_accounting_type_source") }}

    ),
    final as (

        select

            -- ids
            {{
                dbt_utils.surrogate_key(
                    [
                        "schedule_source.revenue_contract_schedule_id",
                        "schedule_source.revenue_contract_line_id",
                        "calendar_source.period_id",
                        "schedule_source.period_id",
                    ]
                )
            }} as dim_waterfall_summary_id,
            schedule_source.revenue_contract_schedule_id
            as dim_revenue_contract_schedule_id,
            schedule_source.revenue_contract_line_id as dim_revenue_contract_line_id,
            schedule_source.accounting_type_id as dim_accounting_type_id,

            -- dates
            {{ get_date_id("calendar_source.period_id") }} as as_of_period_date_id,
            {{ get_date_id("schedule_source.period_id") }} as period_date_id,
            {{ get_date_id("schedule_source.posted_period_id") }}
            as posted_period_date_id,

            schedule_source.amount as transactional_amount,
            schedule_source.amount
            * schedule_source.functional_currency_exchange_rate as functional_amount,
            (schedule_source.amount * schedule_source.functional_currency_exchange_rate)
            * schedule_source.reporting_currency_exchange_rate as reporting_amount,

            -- metadata
            {{ get_date_id("schedule_source.revenue_contract_schedule_created_date") }}
            as revenue_contract_schedule_created_date_id,
            schedule_source.revenue_contract_schedule_created_by,
            {{ get_date_id("schedule_source.revenue_contract_schedule_updated_date") }}
            as revenue_contract_schedule_updated_date_id,
            schedule_source.revenue_contract_schedule_updated_by,
            {{ get_date_id("schedule_source.revenue_contract_schedule_updated_date") }}
            as incremental_update_date,
            schedule_source.security_attribute_value

        from schedule_source
        inner join
            calendar_source
            on schedule_source.revenue_contract_schedule_created_period_id
            <= calendar_source.period_id
            and schedule_source.period_id >= calendar_source.period_id
        inner join
            accounting_type_source
            on schedule_source.accounting_type_id
            = accounting_type_source.accounting_type_id
        where accounting_type_source.is_waterfall_account = 'Y'

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
