{{ config(tags=["product"]) }}

{{
    simple_cte(
        [
            ("namespace_current", "gitlab_dotcom_namespaces_source"),
            ("namespace_snapshots", "prep_namespace_hist"),
            ("namespace_settings", "gitlab_dotcom_namespace_settings_source"),
            (
                "namespace_lineage_historical",
                "gitlab_dotcom_namespace_lineage_historical_daily",
            ),
            ("map_namespace_internal", "map_namespace_internal"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("product_tiers", "prep_product_tier"),
            ("members_source", "gitlab_dotcom_members_source"),
            ("projects_source", "gitlab_dotcom_projects_source"),
            ("audit_events", "gitlab_dotcom_audit_events_source"),
            ("audit_event_details_clean", "prep_audit_event_details_clean"),
            ("users", "prep_user"),
        ]
    )
}},
members as (

    select source_id, count(distinct member_id) as member_count
    from members_source
    where
        is_currently_valid = true
        and member_source_type = 'Namespace'
        and {{ filter_out_blocked_users("members_source", "user_id") }}
    group by 1

),
projects as (

    select namespace_id, count(distinct project_id) as project_count
    from projects_source
    group by 1

),
creators as (

    select author_id as creator_id, entity_id as group_id
    from audit_events
    inner join
        audit_event_details_clean
        on audit_events.audit_event_id = audit_event_details_clean.audit_event_id
    where entity_type = 'Group' and key_name = 'add' and key_value = 'group'
    group by 1, 2

),
namespace_lineage as (

    select
        namespace_lineage_historical.*,
        iff(
            row_number() over (
                partition by namespace_lineage_historical.namespace_id
                order by namespace_lineage_historical.snapshot_day desc
            )
            = 1,
            true,
            false
        ) as is_current,
        iff(
            namespace_lineage_historical.snapshot_day = current_date, true, false
        ) as ultimate_parent_is_current,
        plans.plan_title as ultimate_parent_plan_title,
        plans.plan_is_paid as ultimate_parent_plan_is_paid,
        plans.plan_name as ultimate_parent_plan_name
    from namespace_lineage_historical
    inner join
        plans on namespace_lineage_historical.ultimate_parent_plan_id = plans.plan_id
    qualify
        row_number() over (
            partition by
                namespace_lineage_historical.namespace_id,
                namespace_lineage_historical.parent_id,
                namespace_lineage_historical.ultimate_parent_id
            order by namespace_lineage_historical.snapshot_day desc
        )
        = 1

),
namespaces as (

    select
        namespace_snapshots.*,
        iff(namespace_current.namespace_id is not null, true, false) as is_current
    from namespace_snapshots
    left join
        namespace_current
        on namespace_snapshots.dim_namespace_id = namespace_current.namespace_id

),
joined as (

    select
        namespaces.dim_namespace_id,
        coalesce(
            namespace_lineage.ultimate_parent_id,
            namespaces.parent_id,
            namespaces.dim_namespace_id
        ) as ultimate_parent_namespace_id,
        iff(
            namespaces.dim_namespace_id = coalesce(
                namespace_lineage.ultimate_parent_id,
                namespaces.parent_id,
                namespaces.dim_namespace_id
            ),
            true,
            false
        ) as namespace_is_ultimate_parent,
        iff(
            map_namespace_internal.ultimate_parent_namespace_id is not null, true, false
        ) as namespace_is_internal,
        case
            when namespaces.visibility_level = 'public' or namespace_is_internal
            then namespace_name
            when namespaces.visibility_level = 'internal'
            then 'internal - masked'
            when namespaces.visibility_level = 'private'
            then 'private - masked'
        end as namespace_name,
        case
            when namespaces.visibility_level = 'public' or namespace_is_internal
            then namespace_path
            when namespaces.visibility_level = 'internal'
            then 'internal - masked'
            when namespaces.visibility_level = 'private'
            then 'private - masked'
        end as namespace_path,
        namespaces.owner_id,
        namespaces.namespace_type as namespace_type,
        namespaces.has_avatar,
        namespaces.namespace_created_at as created_at,
        namespaces.namespace_updated_at as updated_at,
        namespaces.is_membership_locked,
        namespaces.has_request_access_enabled,
        namespaces.has_share_with_group_locked,
        namespace_settings.is_setup_for_company,
        namespaces.visibility_level,
        namespaces.ldap_sync_status,
        namespaces.ldap_sync_error,
        namespaces.ldap_sync_last_update_at,
        namespaces.ldap_sync_last_successful_update_at,
        namespaces.ldap_sync_last_sync_at,
        namespaces.lfs_enabled,
        namespaces.parent_id,
        namespaces.shared_runners_enabled,
        namespaces.shared_runners_minutes_limit,
        namespaces.extra_shared_runners_minutes_limit,
        namespaces.repository_size_limit,
        namespaces.does_require_two_factor_authentication,
        namespaces.two_factor_grace_period,
        namespaces.project_creation_level,
        namespaces.push_rule_id,
        ifnull(creators.creator_id, namespaces.owner_id) as creator_id,
        ifnull(users.is_blocked_user, false) as namespace_creator_is_blocked,
        namespace_lineage.ultimate_parent_plan_id as gitlab_plan_id,
        namespace_lineage.ultimate_parent_plan_title as gitlab_plan_title,
        namespace_lineage.ultimate_parent_plan_is_paid as gitlab_plan_is_paid,
        {{ get_keyed_nulls("saas_product_tiers.dim_product_tier_id") }}
        as dim_product_tier_id,
        namespace_lineage.seats as gitlab_plan_seats,
        namespace_lineage.seats_in_use as gitlab_plan_seats_in_use,
        namespace_lineage.max_seats_used as gitlab_plan_max_seats_used,
        ifnull(members.member_count, 0) as namespace_member_count,
        ifnull(projects.project_count, 0) as namespace_project_count,
        ifnull(
            namespaces.is_current and namespace_lineage.is_current, false
        ) as is_currently_valid
    from namespaces
    left join
        namespace_lineage
        on namespaces.dim_namespace_id = namespace_lineage.namespace_id
        and ifnull(namespaces.parent_id, namespaces.dim_namespace_id)
        = ifnull(namespace_lineage.parent_id, namespace_lineage.namespace_id)
    left join
        namespace_settings
        on namespaces.dim_namespace_id = namespace_settings.namespace_id
    left join members on namespaces.dim_namespace_id = members.source_id
    left join projects on namespaces.dim_namespace_id = projects.namespace_id
    left join creators on namespaces.dim_namespace_id = creators.group_id
    left join users on creators.creator_id = users.dim_user_id
    left join
        map_namespace_internal
        on namespace_lineage.ultimate_parent_id
        = map_namespace_internal.ultimate_parent_namespace_id
    left join
        product_tiers saas_product_tiers
        on saas_product_tiers.product_delivery_type = 'SaaS'
        and namespace_lineage.ultimate_parent_plan_name = lower(
            iff(
                saas_product_tiers.product_tier_name_short != 'Trial: Ultimate',
                saas_product_tiers.product_tier_historical_short,
                'ultimate_trial'
            )
        )
    qualify
        row_number() over (
            partition by
                namespaces.dim_namespace_id,
                namespaces.parent_id,
                namespace_lineage.ultimate_parent_id
            order by namespaces.namespace_updated_at desc
        )
        = 1

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@jpeguero",
        created_date="2021-01-14",
        updated_date="2022-02-22",
    )
}}
