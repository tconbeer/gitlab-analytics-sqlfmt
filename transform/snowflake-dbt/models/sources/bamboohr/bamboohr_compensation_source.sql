with
    source as (

        select *
        from {{ source("bamboohr", "compensation") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['id']::number as compensation_update_id,
            data_by_row['employeeId']::number as employee_id,
            data_by_row['startDate']::date as effective_date,
            data_by_row['type']::varchar as compensation_type,
            data_by_row['reason']::varchar as compensation_change_reason,
            data_by_row['paidPer']::varchar as pay_rate,
            data_by_row['rate'] ['value']::float as compensation_value,
            data_by_row['rate'] ['currency']::varchar as compensation_currency,
            uploaded_at
        from intermediate

    )

select
    compensation_update_id,
    employee_id,
    effective_date,
    compensation_type,
    compensation_change_reason,
    pay_rate,
    iff(
        compensation_type = 'Hourly', compensation_value * 80, compensation_value
    ) as compensation_value,
    compensation_currency
from renamed
