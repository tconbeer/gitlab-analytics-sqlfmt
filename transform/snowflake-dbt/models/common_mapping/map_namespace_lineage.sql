{{
    simple_cte(
        [
            ("namespaces_current", "gitlab_dotcom_namespaces_source"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("gitlab_subscriptions", "gitlab_dotcom_gitlab_subscriptions_source"),
        ]
    )
}}

,
active_gitlab_subscriptions as (

    select *
    from gitlab_subscriptions
    where
        is_currently_valid = true and ifnull(
            gitlab_subscription_end_date, current_date
        ) >= current_date

),
namespaces as (

    select namespace_id, parent_id
    from namespaces_current

    UNION ALL
    /*
      Union parent_ids with deleted namespace_ids. These cause their child namespaces to be missed by the top-down recursive CTE.
      Child namespaces with deleted parents are quite rare (n=82 on 2020-01-06, n=113 on 2020-12-17, n=114 on 2021-05-01),
      but need to be included in this model for full coverage.
    */
    select deleted_parents.parent_id as namespace_id, null as parent_id
    from namespaces_current deleted_parents
    left join
        namespaces_current ultimate_parents
        on deleted_parents.parent_id = ultimate_parents.namespace_id
    where
        deleted_parents.parent_id is not null and ultimate_parents.namespace_id is null
    group by 1, 2

),
recursive_namespaces(namespace_id, parent_id, upstream_lineage) as (

    -- Select all namespaces without parents
    -- Initiate lineage array
    select namespace_id, parent_id, to_array(namespace_id) as upstream_lineage
    from namespaces
    where parent_id is null

    UNION ALL

    -- Recursively iterate through each of the children namespaces 
    select
        iter.namespace_id,
        iter.parent_id,
        -- Copy the lineage array of parent, append self to end
        array_append(anchor.upstream_lineage, iter.namespace_id) as upstream_lineage
    from recursive_namespaces as anchor  -- Parent namespace
    -- Child namespace
    inner join namespaces as iter on anchor.namespace_id = iter.parent_id

),
extracted as (

    select
        recursive_namespaces.*,
        recursive_namespaces.upstream_lineage[  -- First item is the ultimate parent.
            0
        ]::number as ultimate_parent_namespace_id,
        iff(
            namespaces_current.namespace_id is not null, true, false
        ) as is_currently_valid
    from recursive_namespaces
    left join
        namespaces_current
        on recursive_namespaces.namespace_id = namespaces_current.namespace_id
    where recursive_namespaces.namespace_id != 0

),
with_plans as (

    select
        extracted.namespace_id as dim_namespace_id,
        extracted.parent_id as dim_namespace_parent_id,
        extracted.ultimate_parent_namespace_id as dim_namespace_ultimate_parent_id,
        extracted.upstream_lineage as upstream_lineage,
        extracted.is_currently_valid as is_currently_valid,
        namespace_plans.plan_id as namespace_plan_id,
        namespace_plans.plan_title as namespace_plan_title,
        namespace_plans.plan_is_paid as namespace_plan_is_paid,
        iff(
            -- Excluded Premium (103) and Free (34) Trials from being remapped as
            -- Ultimate Trials
            ultimate_parent_gitlab_subscriptions.is_trial and ifnull(
                ultimate_parent_gitlab_subscriptions.plan_id, 34
            ) not in (34, 103),
            -- All historical trial GitLab subscriptions were Ultimate/Gold Trials
            -- (102)
            102,
            ifnull(ultimate_parent_plans.plan_id, 34)
        ) as ultimate_parent_plan_id,
        iff(
            ultimate_parent_plan_id = 102,
            'Ultimate Trial',
            ifnull(ultimate_parent_plans.plan_title, 'Free')
        ) as ultimate_parent_plan_title,
        iff(
            ultimate_parent_gitlab_subscriptions.is_trial,
            false,
            ifnull(ultimate_parent_plans.plan_is_paid, false)
        ) as ultimate_parent_plan_is_paid
    from extracted
    -- Get plan information for the namespace.
    left join
        active_gitlab_subscriptions as namespace_gitlab_subscriptions
        on extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
    left join
        plans as namespace_plans on ifnull(
            namespace_gitlab_subscriptions.plan_id, 34
        ) = namespace_plans.plan_id
    -- Get plan information for the ultimate parent namespace.
    left join
        active_gitlab_subscriptions as ultimate_parent_gitlab_subscriptions
        on extracted.ultimate_parent_namespace_id
        = ultimate_parent_gitlab_subscriptions.namespace_id
    left join
        plans as ultimate_parent_plans on ifnull(
            ultimate_parent_gitlab_subscriptions.plan_id, 34
        ) = ultimate_parent_plans.plan_id

)

{{
    dbt_audit(
        cte_ref="with_plans",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-06-16",
        updated_date="2021-06-16",
    )
}}
