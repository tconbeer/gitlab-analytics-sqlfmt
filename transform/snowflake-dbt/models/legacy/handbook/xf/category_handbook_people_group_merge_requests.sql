{{ config({"materialized": "view"}) }}

with
    handbook_categories as (

        select * from {{ ref("category_handbook_merge_requests") }}

    ),
    filtered_to_people_group as (

        select *
        from handbook_categories
        where array_contains('people_group'::variant, merge_request_department_list)

    )
select *
from filtered_to_people_group
