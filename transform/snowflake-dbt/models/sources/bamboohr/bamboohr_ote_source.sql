{{ config({"materialized": "table"}) }}

with
    source as (

        select *
        from {{ source("bamboohr", "custom_on_target_earnings") }}
        order by uploaded_at desc
        limit 1

    ),
    renamed as (

        select
            data_by_row.value['id']::number as target_earnings_update_id,
            data_by_row.value['employeeId']::number as employee_id,
            data_by_row.value['customDate']::date as effective_date,
            data_by_row.value['customAnnualAmountLocal']::varchar
            as annual_amount_local,
            data_by_row.value['customAnnualAmountUSD']::varchar as annual_amount_usd,
            data_by_row.value['customOTELocal']::varchar as ote_local,
            data_by_row.value['customOTEUSD']::varchar as ote_usd,
            data_by_row.value['customType']::varchar as ote_type,
            data_by_row.value['customVariablePay']::varchar as variable_pay
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) data_by_row

    ),
    final as (

        select
            target_earnings_update_id,
            employee_id,
            effective_date,
            variable_pay,
            annual_amount_local,
            split_part(annual_amount_usd, ' ', 1) as annual_amount_usd_value,
            ote_local,
            split_part(ote_usd, ' ', 1) as ote_usd,
            ote_type
        from renamed

    )

select
    *,
    lag(coalesce(annual_amount_usd_value, 0)) over (
        partition by employee_id order by target_earnings_update_id
    ) as prior_annual_amount_usd,
    annual_amount_usd_value - prior_annual_amount_usd as change_in_annual_amount_usd
from final
where
    target_earnings_update_id
    != 23721  -- incorrect order
