{{ config({"materialized": "view"}) }}

with
    handbook_categories as (

        select * from {{ ref("category_handbook_merge_requests") }}

    ),
    filtered_to_engineering as (

        select *
        from handbook_categories
        where
            array_contains('engineering'::variant, merge_request_department_list)
            or array_contains('support'::variant, merge_request_department_list)

    )
select *
from filtered_to_engineering
