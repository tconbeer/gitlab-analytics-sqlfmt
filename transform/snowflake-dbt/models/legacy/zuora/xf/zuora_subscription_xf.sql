with
    zuora_subscription_intermediate as (

        select * from {{ ref("zuora_subscription_intermediate") }}

    ),
    zuora_subscription_lineage as (

        select * from {{ ref("zuora_subscription_lineage") }}

    ),
    zuora_subscription_parentage as (

        select * from {{ ref("zuora_subscription_parentage_finish") }}

    )

select
    zuora_subscription_intermediate.*,
    zuora_subscription_lineage.lineage,
    coalesce(
        zuora_subscription_parentage.ultimate_parent_sub,
        zuora_subscription_intermediate.subscription_name_slugify
    ) as oldest_subscription_in_cohort,
    coalesce(
        zuora_subscription_parentage.cohort_month,
        zuora_subscription_intermediate.subscription_month
    ) as cohort_month,
    coalesce(
        zuora_subscription_parentage.cohort_quarter,
        zuora_subscription_intermediate.subscription_quarter
    ) as cohort_quarter,
    coalesce(
        zuora_subscription_parentage.cohort_year,
        zuora_subscription_intermediate.subscription_year
    ) as cohort_year
from zuora_subscription_intermediate
left join
    zuora_subscription_lineage
    on zuora_subscription_intermediate.subscription_name_slugify
    =
    zuora_subscription_lineage.subscription_name_slugify
left join
    zuora_subscription_parentage
    on zuora_subscription_intermediate.subscription_name_slugify
    =
    zuora_subscription_parentage.child_sub
