{% set lines_to_repeat %}
     date_actual,
     SUM(weighted_deviated_from_comp_calc) AS sum_weighted_deviated_from_comp_calc,
     COUNT(DISTINCT(employee_number))      AS current_employees,
     sum_weighted_deviated_from_comp_calc/
        current_employees                  AS percent_of_employees_outside_of_band
    FROM joined
    WHERE date_actual < CURRENT_DATE
    GROUP BY 1,2,3,4
{% endset %}

with
    employee_directory_intermediate as (

        select * from {{ ref("employee_directory_intermediate") }}

    ),
    comp_band as (select * from {{ ref("comp_band_deviation_snapshots") }}),
    date_details as (

        select distinct last_day_of_month
        from {{ ref("dim_date") }}
        where  -- last day we captured before transitioning to new report
            (
                -- started capturing again from new report
                last_day_of_month < '2020-05-20' or last_day_of_month >= '2020-10-31'
            ) and last_day_of_month <= current_date()

    ),
    joined as (

        select
            employee_directory_intermediate.*,
            comp_band.deviation_from_comp_calc,
            comp_band.original_value,
            case
                when lower(original_value) = 'exec'
                then 0
                when deviation_from_comp_calc <= 0.0001
                then 0
                when deviation_from_comp_calc <= 0.05
                then 0.25
                when deviation_from_comp_calc <= 0.1
                then 0.5
                when deviation_from_comp_calc <= 0.15
                then 0.75
                when deviation_from_comp_calc <= 1
                then 1
                else null
            end as weighted_deviated_from_comp_calc
        from employee_directory_intermediate
        left join
            comp_band
            on employee_directory_intermediate.employee_number
            = comp_band.employee_number
            and valid_from <= date_actual
            and coalesce(
                valid_to::date, {{ max_date_in_bamboo_analyses() }}
            ) > date_actual

    ),
    department_aggregated as (

        select
            'department_breakout' as breakout_type,
            division_mapped_current as division,
            department,
            {{ lines_to_repeat }}

    ),
    division_aggregated as (

        select
            'division_breakout' as breakout_type,
            division_mapped_current as division,
            'division_breakout' as department,
            {{ lines_to_repeat }}

    ),
    company_aggregated as (

        select
            'company_breakout' as breakout_type,
            'Company - Overall' as division,
            'company_breakout' as department,
            {{ lines_to_repeat }}

    ),
    unioned as (

        select *
        from department_aggregated
        UNION ALL

        select *
        from division_aggregated

        UNION ALL

        select *
        from company_aggregated

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["date_actual", "breakout_type", "division", "department"]
                )
            }} as unique_key, unioned.*
        from unioned
        inner join date_details on unioned.date_actual = date_details.last_day_of_month
        where date_actual > '2019-01-01'

    )

select *
from final
order by 1
