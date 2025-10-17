{{ config({"schema": "legacy"}) }}

with
    source as (select * from {{ ref("zuora_subscription_lineage") }}),
    zuora_subscription_intermediate as (

        select * from {{ ref("zuora_subscription_intermediate") }}

    ),
    flattened as (

        select
            subscription_name_slugify,
            c.value::varchar as subscriptions_in_lineage,
            c.index as child_index
        from source, lateral flatten(input => split(lineage, ',')) c

    ),
    find_max_depth as (

        select subscriptions_in_lineage, max(child_index) as child_index
        from flattened
        group by 1

    ),
    with_parents as (

        select
            subscription_name_slugify as ultimate_parent_sub,
            find_max_depth.subscriptions_in_lineage as child_sub,
            find_max_depth.child_index as depth
        from find_max_depth
        left join
            flattened
            on find_max_depth.subscriptions_in_lineage
            = flattened.subscriptions_in_lineage
            and find_max_depth.child_index = flattened.child_index

    ),
    finalish as (

        select
            with_parents.ultimate_parent_sub,
            with_parents.child_sub,
            zuora_subscription_intermediate.subscription_month as cohort_month,
            zuora_subscription_intermediate.subscription_quarter as cohort_quarter,
            zuora_subscription_intermediate.subscription_year as cohort_year
        from with_parents
        left join
            zuora_subscription_intermediate
            on zuora_subscription_intermediate.subscription_name_slugify
            = with_parents.ultimate_parent_sub

    )
select *
from finalish
