with
    source as (

        select *
        from {{ source("bamboohr", "custom_currency_conversion") }}
        order by uploaded_at desc
        limit 1

    ),
    renamed as (

        select
            data_by_row.value['id']::number as conversion_id,
            data_by_row.value['employeeId']::number as employee_id,
            data_by_row.value['customConversionEffectiveDate']::date as effective_date,
            data_by_row.value['customCurrencyConversionFactor']::decimal(
                10, 5
            ) as currency_conversion_factor,
            data_by_row.value[
                'customLocalAnnualSalary'
            ]::varchar as local_annual_salary,
            data_by_row.value['customUSDAnnualSalary']::varchar as usd_annual_salary,
            uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) data_by_row

    ),
    final as (

        select
            conversion_id,
            employee_id,
            effective_date,
            currency_conversion_factor,
            local_annual_salary,
            regexp_replace(
                local_annual_salary, '[0-9/-/#/./*]', ''
            ) as annual_local_currency_code,
            regexp_replace(usd_annual_salary, '[a-z/-/A-z/#/*]', '')::decimal(
                10, 2
            ) as annual_amount_usd_value,
            regexp_replace(
                usd_annual_salary, '[0-9/-/#/./*]', ''
            ) as annual_local_usd_code
        from renamed

    )

select *
from final
