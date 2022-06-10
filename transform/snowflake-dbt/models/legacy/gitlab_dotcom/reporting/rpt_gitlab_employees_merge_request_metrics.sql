{% set lines_to_repeat %}
      month_date,
      COUNT(DISTINCT(intermediate.employee_id))                                             AS total_employees,
      COUNT(DISTINCT(IFF(is_part_of_product = TRUE, intermediate.employee_id, NULL)))       AS total_employees_with_mr_part_of_product,
      COUNT(IFF(is_part_of_product = TRUE, merge_request_id, NULL))                         AS total_merged_part_of_product,
      COUNT(IFF(is_part_of_product = TRUE AND 
                merge_request_data_source = 'gitlab_dotcom',merge_request_id,NULL))         AS total_gitlab_dotcom_product_merge_requests,
      COUNT(IFF(is_part_of_product = TRUE 
                AND merge_request_data_source = 'gitlab_ops',merge_request_id,NULL))        AS total_gitlab_ops_product_merge_requests,
      ROUND((total_merged_part_of_product/total_employees),2)                               AS narrow_mr_rate,
      total_employees_with_mr_part_of_product/total_employees                               AS percent_of_employees_with_mr,
      SUM(people_engineering_project)                                                       AS total_people_engineering_merge_requests
    FROM intermediate
{% endset %}

with
    merge_requests as (

        select *
        from {{ ref("gitlab_employees_merge_requests_xf") }}
        where merged_at is not null

    ),
    employees as (select * from {{ ref("gitlab_bamboohr_employee_base") }}),
    intermediate as (

        select
            employees.*,
            merge_requests.merge_request_id,
            merge_requests.merge_request_data_source,
            merge_requests.merged_at,
            merge_requests.is_part_of_product,
            people_engineering_project
        from employees
        left join
            merge_requests
            on merge_requests.bamboohr_employee_id = employees.employee_id
            and date_trunc(
                day, merge_requests.merged_at
            ) between employees.valid_from and coalesce(
                employees.valid_to, current_date()
            )

    ),
    aggregated as (

        select
            'division' as breakout_level,
            division,
            null as department,
            null as employee_id,
            {{ lines_to_repeat }}
            {{ dbt_utils.group_by(n=5) }}

        UNION ALL

        select
            'department' as breakout_level,
            division,
            department,
            null as employee_id,
            {{ lines_to_repeat }}
            {{ dbt_utils.group_by(n=5) }}

        UNION ALL

        select
            'team_member' as breakout_level,
            division,
            department,
            employee_id,
            {{ lines_to_repeat }}
            {{ dbt_utils.group_by(n=5) }}

        UNION ALL

        select
            'division_modified' as breakout_level,
            'R&D_engineering_and_product' as division,
            null as department,
            null as employee_id,
            {{ lines_to_repeat }}
        where
            division in ('Engineering', 'Product') and department != 'Customer Support'
            {{ dbt_utils.group_by(n=5) }}

    )

select *
from aggregated
