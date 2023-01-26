{{ config({"database": env_var("SNOWFLAKE_PROD_DATABASE"), "schema": "legacy"}) }}
with recursive
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions") }}
        where is_currently_valid = true

    ),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }}),
    recursive_namespaces(namespace_id, parent_id, upstream_lineage) as (

        -- Select all namespaces without parents
        select
            namespace_id,
            namespaces.parent_id,
            to_array(namespace_id) as upstream_lineage  -- Initiate lineage array
        from namespaces
        where namespaces.parent_id is null

        union all

        -- Recursively iterate through each of the children namespaces
        select
            iter.namespace_id,
            iter.parent_id,
            array_insert(
                anchor.upstream_lineage, 0, iter.namespace_id
            -- Copy the lineage array of parent, inserting self at start
            ) as upstream_lineage
        from recursive_namespaces as anchor  -- Parent namespace
        inner join
            namespaces as iter  -- Child namespace
            on anchor.namespace_id = iter.parent_id

    ),
    extracted as (

        select
            *,
            get(
                upstream_lineage, array_size(upstream_lineage) - 1
            ) as ultimate_parent_id  -- Last item is the ultimate parent.
        from recursive_namespaces

        union all
        /* Union all children with deleted ancestors. These are missed by the top-down recursive CTE.
     This is quite rare (n=82 on 2020-01-06) but need to be included in this model for full coverage. */
        select
            namespaces.namespace_id,
            namespaces.parent_id,
            array_construct() as upstream_lineage,  -- Empty Array.
            0 as ultimate_parent_id
        from namespaces
        where
            parent_id not in (select distinct namespace_id from namespaces)
            or namespace_id in (
                11967197,
                11967195,
                11967194,
                11967196,
                12014338,
                12014366,
                6713278,
                6142621,
                4159925,
                8370670,
                8370671,
                8437164,
                8437147,
                8437148,
                8437172,
                8437156,
                8437159,
                8437146,
                8437176,
                8437165,
                8437179,
                8427708,
                8437167,
                8437110,
                8437178,
                8437175,
                8427717,
                8437153,
                8437161,
                8437169,
                8437177,
                8437160,
                8437157,
                8437154,
                8437162,
                8437150,
                8437149,
                8427716,
                8437142,
                8437145,
                8437151,
                8437171,
                8437155,
                8437173,
                8437170
            )  -- Grandparent or older is deleted.

    ),
    with_plans as (

        select
            extracted.*,
            coalesce(
                (ultimate_parent_id in {{ get_internal_parent_namespaces() }}), false
            ) as namespace_is_internal,
            namespace_plans.plan_id as namespace_plan_id,
            namespace_plans.plan_title as namespace_plan_title,
            namespace_plans.plan_is_paid as namespace_plan_is_paid,
            coalesce(ultimate_parent_plans.plan_id, 34) as ultimate_parent_plan_id,
            case
                when
                    ultimate_parent_gitlab_subscriptions.is_trial
                    and coalesce(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34
                then 'Trial: Ultimate'
                else coalesce(ultimate_parent_plans.plan_title, 'Free')
            end as ultimate_parent_plan_title,
            case
                when ultimate_parent_gitlab_subscriptions.is_trial
                then false
                else coalesce(ultimate_parent_plans.plan_is_paid, false)
            end as ultimate_parent_plan_is_paid
        from extracted
        -- Get plan information for the namespace.
        left join
            gitlab_subscriptions as namespace_gitlab_subscriptions
            on extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
        left join
            plans as namespace_plans
            on coalesce(namespace_gitlab_subscriptions.plan_id, 34)
            = namespace_plans.plan_id
        -- Get plan information for the ultimate parent namespace.
        left join
            gitlab_subscriptions as ultimate_parent_gitlab_subscriptions
            on extracted.ultimate_parent_id
            = ultimate_parent_gitlab_subscriptions.namespace_id
        left join
            plans as ultimate_parent_plans
            on coalesce(ultimate_parent_gitlab_subscriptions.plan_id, 34)
            = ultimate_parent_plans.plan_id

    )

select *
from with_plans
