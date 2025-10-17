with
    source as (select * from {{ source("sheetload", "rep_quotas_full_ps_fy2020") }}),
    final as (

        select
            sales_rep,
            type,
            team,
            fiscal_year::number as fiscal_year,
            zeroifnull(nullif("PS_QUOTA", '')::number(16, 5)) as ps_quota,
            bamboo_employee_id::number as bamboo_employee_id
        from source

    )

select *
from final
