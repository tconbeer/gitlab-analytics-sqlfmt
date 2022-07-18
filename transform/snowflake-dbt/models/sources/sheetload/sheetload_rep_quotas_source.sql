with
    source as (select * from {{ source("sheetload", "rep_quotas") }}),
    final as (

        select
            bamboo_employee_id,
            sfdc_user_id,
            calendar_month::date as calendar_month,
            fiscal_quarter::number as fiscal_quarter,
            fiscal_year::number as fiscal_year,
            adjusted_start_date::date as adjusted_start_date,
            case
                when trim(full_quota) in ('NA', '#N/A')
                then 0
                else zeroifnull(trim(full_quota)::number(16, 5))
            end as full_quota,
            case
                when trim(ramping_quota) in ('', '#N/A')
                then 0
                else zeroifnull(trim(ramping_quota)::number(16, 5))
            end as ramping_quota,
            zeroifnull(
                nullif(trim(ramping_percent), '')::number(3, 2)
            ) as ramping_percent,
            zeroifnull(
                nullif(trim(seasonality_percent), '')::number(3, 2)
            ) as seasonality_percent,
            zeroifnull(
                nullif(trim(gross_iacv_attainment), '')::number(16, 2)
            ) as gross_iacv_attainment,
            zeroifnull(
                nullif(trim(net_iacv_attainment), '')::number(16, 2)
            ) as net_iacv_attainment,
            sales_rep,
            team,
            type
        from source

    )

select *
from final
