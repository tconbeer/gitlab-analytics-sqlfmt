{{ config({"materialized": "ephemeral"}) }}

with
    source as (

        select *
        from {{ source("snapshots", "sheetload_employee_location_factor_snapshots") }}
        where
            "Employee_ID" != 'Not In Comp Calc' and "Employee_ID" not in (
                '$72,124', 'S1453'
            )

    ),
    renamed as (

        select
            nullif("Employee_ID", '')::varchar as employee_number,
            deviation_from_comp_calc as original_value,
            case
                when nullif(deviation_from_comp_calc, '') = 'Exec'
                then '0.00'
                when nullif(deviation_from_comp_calc, '') = '#DIV/0!'
                then null
                when deviation_from_comp_calc like '%'
                then nullif(replace (deviation_from_comp_calc, '%', ''), '')
                else nullif(deviation_from_comp_calc, '')
            end as deviation_from_comp_calc_cl,
            iff(
                "DBT_VALID_FROM"::number::timestamp::date < '2019-10-18'::date,
                '2000-01-20'::date,
                "DBT_VALID_FROM"::number::timestamp::date
            ) as valid_from,
            "DBT_VALID_TO"::number::timestamp::date as valid_to
        from source
        where deviation_from_comp_calc_cl is not null

    ),
    deduplicated as (

        select distinct
            employee_number,
            original_value,
            iff(
                contains(original_value, '%') = true,
                round(deviation_from_comp_calc_cl / 100::float, 4),
                round(deviation_from_comp_calc_cl::float, 4)
            ) as deviation_from_comp_calc,
            valid_from,
            valid_to
        from renamed

    ),
    final as (

        select
            employee_number,
            original_value,
            deviation_from_comp_calc,
            min(valid_from) as valid_from,
            coalesce(max(valid_to), '2020-05-20') as valid_to
        -- -last day we captured from this sheetload tab--
        from deduplicated
        group by 1, 2, 3

    )

select *
from final
