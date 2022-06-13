{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ ref("bamboohr_custom_bonus_source") }}),
    current_division_department_mapping as (

        select * from {{ ref("bamboohr_job_info_current_division_base") }}

    ),
    filtered as (

        select source.*, department, division_mapped_current as division
        from source
        left join
            current_division_department_mapping
            on source.employee_id = current_division_department_mapping.employee_id
            and source.bonus_date
            between current_division_department_mapping.effective_date
            and coalesce(
                current_division_department_mapping.effective_end_date::date,
                {{ max_date_in_bamboo_analyses() }}
            )
        where source.bonus_type = 'Discretionary Bonus'

    )

select *
from filtered
