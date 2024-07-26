{{ config({"materialized": "view"}) }}

with
    handbook_categories as (

        select * from {{ ref("category_handbook_merge_requests") }}

    ),
    filtered_to_business_technology as (

        select *
        from handbook_categories
        where
            array_contains(
                'business_technology'::variant, merge_request_department_list
            )
            or array_contains('procurement'::variant, merge_request_department_list)

    )

select *
from filtered_to_business_technology
