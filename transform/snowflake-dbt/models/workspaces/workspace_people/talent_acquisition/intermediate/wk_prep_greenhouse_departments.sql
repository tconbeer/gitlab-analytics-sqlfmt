
{{ config(materialized="ephemeral") }}

with
    source as (select * from {{ ref("greenhouse_departments_source") }}),
    greenhouse_departments(
        department_name, department_id, hierarchy_id, hierarchy_name
    ) as (
        select
            department_name,
            department_id,
            to_array(department_id) as hierarchy_id,
            to_array(department_name) as hierarchy_name
        from source
        where parent_id is null
        UNION ALL
        select
            iteration.department_name,
            iteration.department_id,
            array_append(anchor.hierarchy_id, iteration.department_id) as hierarchy_id,
            array_append(
                anchor.hierarchy_name, iteration.department_name
            ) as hierarchy_name
        from source iteration
        inner join
            greenhouse_departments anchor on iteration.parent_id = anchor.department_id
    )
select
    department_name,
    department_id,
    array_size(hierarchy_id) as hierarchy_level,
    hierarchy_id,
    hierarchy_name,
    hierarchy_name[0]::varchar as level_1,
    hierarchy_name[1]::varchar as level_2,
    hierarchy_name[2]::varchar as level_3
from greenhouse_departments
