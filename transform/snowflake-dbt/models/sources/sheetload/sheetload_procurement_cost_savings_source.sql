with
    source as (select * from {{ source("sheetload", "procurement_cost_savings") }}),
    renamed as (

        select
            calendar_month::date as calendar_month,
            try_to_decimal(replace(replace(savings, '$'), ','), 14, 2) as savings,
            try_to_decimal(
                replace(replace(rolling_12_month_savings_without_audit, '$'), ','),
                14,
                2
            ) as rolling_12_month_savings_without_audit,
            try_to_decimal(
                replace(replace(rolling_12_month_savings_with_audit, '$'), ','), 14, 2
            ) as rolling_12_month_savings_with_audit,
            try_to_decimal(replace(replace(target, '$'), ','), 14, 2) as target
        from source

    )

select *
from renamed
