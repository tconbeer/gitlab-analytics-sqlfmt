{{ config({"schema": "legacy"}) }}

{% set partition_statement = "OVER ( PARTITION BY child_sub ORDER BY cohort_month, ultimate_parent_sub)" %}
-- we have this second "order by" in case of two parents having the same cohort month.
with
    base as (select * from {{ ref("zuora_subscription_parentage_start") }}),
    new_base as (

        select *
        from base
        where child_sub in (select child_sub from base group by 1 having count(*) > 1)

    ),
    consolidated_parents as (

        select
            first_value(
                ultimate_parent_sub
            ) {{ partition_statement }} as ultimate_parent_sub_2,
            child_sub,
            min(cohort_month) {{ partition_statement }} as cohort_month,
            min(cohort_quarter) {{ partition_statement }} as cohort_quarter,
            min(cohort_year) {{ partition_statement }} as cohort_year
        from new_base

    ),
    deduped_consolidations as (

        select * from consolidated_parents group by 1, 2, 3, 4, 5

    ),
    unioned as (

        select
            deduped_consolidations.ultimate_parent_sub_2 as ultimate_parent_sub,
            new_base.ultimate_parent_sub as child_sub,
            deduped_consolidations.cohort_month,
            deduped_consolidations.cohort_quarter,
            deduped_consolidations.cohort_year
        from deduped_consolidations
        left join new_base on new_base.child_sub = deduped_consolidations.child_sub
        where ultimate_parent_sub_2 != ultimate_parent_sub

        union all

        select *
        from deduped_consolidations

        union all

        select *
        from base
        where child_sub not in (select child_sub from new_base)

    ),
    fix_consolidations as (

        select
            first_value(
                ultimate_parent_sub
            ) {{ partition_statement }} as ultimate_parent_sub,
            child_sub,
            min(cohort_month) {{ partition_statement }} as cohort_month,
            min(cohort_quarter) {{ partition_statement }} as cohort_quarter,
            min(cohort_year) {{ partition_statement }} as cohort_year
        from unioned

    )

select *
from fix_consolidations
group by 1, 2, 3, 4, 5
