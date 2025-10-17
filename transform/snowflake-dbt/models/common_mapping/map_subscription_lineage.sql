with
    zuora_subscription_intermediate as (

        select * from {{ ref("prep_subscription_lineage_intermediate") }}

    ),
    zuora_subscription_lineage as (

        select * from {{ ref("prep_subscription_lineage") }}

    ),
    zuora_subscription_parentage as (

        select * from {{ ref("prep_subscription_lineage_parentage_finish") }}

    ),
    final as (

        select
            zuora_subscription_intermediate.subscription_id as dim_subscription_id,
            zuora_subscription_intermediate.subscription_name_slugify
            as subscription_name_slugify,
            zuora_subscription_lineage.lineage as subscription_lineage,
            coalesce(
                zuora_subscription_parentage.ultimate_parent_sub,
                zuora_subscription_intermediate.subscription_name_slugify
            ) as oldest_subscription_in_cohort,
            coalesce(
                zuora_subscription_parentage.cohort_month,
                zuora_subscription_intermediate.subscription_month
            ) as subscription_cohort_month,
            coalesce(
                zuora_subscription_parentage.cohort_quarter,
                zuora_subscription_intermediate.subscription_quarter
            ) as subscription_cohort_quarter,
            coalesce(
                zuora_subscription_parentage.cohort_year,
                zuora_subscription_intermediate.subscription_year
            ) as subscription_cohort_year
        from zuora_subscription_intermediate
        left join
            zuora_subscription_lineage
            on zuora_subscription_intermediate.subscription_name_slugify
            = zuora_subscription_lineage.subscription_name_slugify
        left join
            zuora_subscription_parentage
            on zuora_subscription_intermediate.subscription_name_slugify
            = zuora_subscription_parentage.child_sub

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@paul_armstrong",
            updated_by="@iweeks",
            created_date="2021-02-11",
            updated_date="2021-07-29",
        )
    }}
