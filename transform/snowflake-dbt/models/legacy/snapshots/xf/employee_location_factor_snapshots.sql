{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
            "materialized": "table",
        }
    )
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

with
    source as (

        select *
        from {{ source("snapshots", "sheetload_employee_location_factor_snapshots") }}
        where
            "Employee_ID" != 'Not In Comp Calc'
            and "Employee_ID" not in ('$72,124', 'S1453')

    ),
    renamed as (

        select
            nullif("Employee_ID", '')::varchar as bamboo_employee_number,
            nullif("Location_Factor", '') as location_factor,
            case
                when "DBT_VALID_FROM"::number::timestamp::date < '2019-07-20'::date
                then '2000-01-20'::date
                else "DBT_VALID_FROM"::number::timestamp::date
            end as valid_from,
            "DBT_VALID_TO"::number::timestamp::date as valid_to
        from source
        where
            lower(bamboo_employee_number) not like '%not in comp calc%'
            and location_factor is not null

    ),
    employee_locality as (select * from {{ ref("employee_locality") }}),
    unioned as (

        select
            bamboo_employee_number::number as bamboo_employee_number,
            null as locality,
            location_factor::float as location_factor,
            valid_from,
            valid_to
        from renamed
        where valid_from < '2020-03-24'
        -- -from 2020.03.24 we start capturing this data from bamboohr
        union all

        select
            employee_number,
            bamboo_locality,
            location_factor as location_factor,
            updated_at,
            lead(updated_at) over (
                partition by employee_number order by updated_at
            ) as valid_to
        from employee_locality

    ),
    intermediate as (

        select
            bamboo_employee_number as bamboo_employee_number,
            locality,
            location_factor as location_factor,
            lead(location_factor) over
            (partition by bamboo_employee_number order by valid_from
            ) as next_location_factor,
            valid_from,
            coalesce(valid_to, {{ max_date_in_analysis }}) as valid_to
        from unioned

    ),
    deduplicated as (

        select *
        from intermediate
        qualify
            row_number() over (
                partition by
                    bamboo_employee_number,
                    locality,
                    location_factor,
                    next_location_factor
                order by valid_from
            )
            = 1

    ),
    final as (

        select
            bamboo_employee_number,
            locality,
            location_factor,
            valid_from as valid_from,
            coalesce(
                lead(dateadd(day, -1, valid_from)) over (
                    partition by bamboo_employee_number order by valid_from
                ),
                {{ max_date_in_analysis }}
            ) as valid_to
        from deduplicated
        group by 1, 2, 3, 4

    )

select *
from final
