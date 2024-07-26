{{ config({"materialized": "view"}) }}

with
    marketing_categories as (

        select * from {{ ref("category_marketing_site_merge_requests") }}

    ),
    filtered_to_security as (

        select *
        from marketing_categories
        where array_contains('security'::variant, merge_request_department_list)

    )
select *
from filtered_to_security
