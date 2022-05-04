{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('namespace_current', 'gitlab_dotcom_namespaces_source'),
    ('namespace_snapshots', 'prep_namespace_hist'),
    ('namespace_settings', 'gitlab_dotcom_namespace_settings_source'),
    ('namespace_lineage_historical', 'gitlab_dotcom_namespace_lineage_historical_daily'),
    ('map_namespace_internal', 'map_namespace_internal'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('product_tiers', 'prep_product_tier'),
    ('members_source', 'gitlab_dotcom_members_source'),
    ('projects_source', 'gitlab_dotcom_projects_source'),
    ('audit_events', 'gitlab_dotcom_audit_events_source'),
    ('audit_event_details_clean', 'prep_audit_event_details_clean'),
    ('users', 'prep_user')
]) }}

, members AS (

    SELECT
      source_id,
      COUNT(DISTINCT member_id)                                                       AS member_count
    FROM members_source
    WHERE is_currently_valid = TRUE
      AND member_source_type = 'Namespace'
      AND {{ filter_out_blocked_users('members_source', 'user_id') }}
    GROUP BY 1

), projects AS (

    SELECT
      namespace_id,
      COUNT(DISTINCT project_id)                                                      AS project_count
    FROM projects_source
    GROUP BY 1

), creators AS (

    SELECT
      author_id                                                                       AS creator_id,
      entity_id                                                                       AS group_id
    FROM audit_events
    INNER JOIN audit_event_details_clean
      ON audit_events.audit_event_id = audit_event_details_clean.audit_event_id
    WHERE entity_type = 'Group'
      AND key_name = 'add'
      AND key_value = 'group'
    GROUP BY 1, 2

), namespace_lineage AS (

    SELECT
      namespace_lineage_historical.*,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY namespace_lineage_historical.namespace_id
            ORDER BY namespace_lineage_historical.snapshot_day DESC) = 1,
          TRUE, FALSE)                                                                AS is_current,
      IFF(namespace_lineage_historical.snapshot_day = CURRENT_DATE,
          TRUE, FALSE)                                                                AS ultimate_parent_is_current,
      plans.plan_title                                                                AS ultimate_parent_plan_title,
      plans.plan_is_paid                                                              AS ultimate_parent_plan_is_paid,
      plans.plan_name                                                                 AS ultimate_parent_plan_name
    FROM namespace_lineage_historical
    INNER JOIN plans
      ON namespace_lineage_historical.ultimate_parent_plan_id = plans.plan_id
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        namespace_lineage_historical.namespace_id,
        namespace_lineage_historical.parent_id,
        namespace_lineage_historical.ultimate_parent_id
      ORDER BY namespace_lineage_historical.snapshot_day DESC
    ) = 1

), namespaces AS (

    SELECT
      namespace_snapshots.*,
      IFF(namespace_current.namespace_id IS NOT NULL,
          TRUE, FALSE)                                                                AS is_current
    FROM namespace_snapshots
    LEFT JOIN namespace_current
      ON namespace_snapshots.dim_namespace_id = namespace_current.namespace_id

), joined AS (

    SELECT
      namespaces.dim_namespace_id,
      COALESCE(namespace_lineage.ultimate_parent_id,
               namespaces.parent_id,
               namespaces.dim_namespace_id)                                           AS ultimate_parent_namespace_id,
      IFF(namespaces.dim_namespace_id = COALESCE(namespace_lineage.ultimate_parent_id,
                                                 namespaces.parent_id,
                                                 namespaces.dim_namespace_id),
          TRUE, FALSE)                                                                AS namespace_is_ultimate_parent,
      IFF(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL,
          TRUE, FALSE)                                                                AS namespace_is_internal,
      CASE
        WHEN namespaces.visibility_level = 'public'
          OR namespace_is_internal                    THEN namespace_name
        WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
        WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END                                                                             AS namespace_name,
      CASE
       WHEN namespaces.visibility_level = 'public'
         OR namespace_is_internal                     THEN namespace_path
       WHEN namespaces.visibility_level = 'internal'  THEN 'internal - masked'
       WHEN namespaces.visibility_level = 'private'   THEN 'private - masked'
      END                                                                             AS namespace_path,
      namespaces.owner_id,
      namespaces.namespace_type                                                       AS namespace_type,
      namespaces.has_avatar,
      namespaces.namespace_created_at                                                 AS created_at,
      namespaces.namespace_updated_at                                                 AS updated_at,
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
      IFNULL(creators.creator_id, namespaces.owner_id)                                AS creator_id,
      IFNULL(users.is_blocked_user, FALSE)                                            AS namespace_creator_is_blocked,
      namespace_lineage.ultimate_parent_plan_id                                       AS gitlab_plan_id,
      namespace_lineage.ultimate_parent_plan_title                                    AS gitlab_plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid                                  AS gitlab_plan_is_paid,
      {{ get_keyed_nulls('saas_product_tiers.dim_product_tier_id') }}                 AS dim_product_tier_id,
      namespace_lineage.seats                                                         AS gitlab_plan_seats,
      namespace_lineage.seats_in_use                                                  AS gitlab_plan_seats_in_use,
      namespace_lineage.max_seats_used                                                AS gitlab_plan_max_seats_used,
      IFNULL(members.member_count, 0)                                                 AS namespace_member_count,
      IFNULL(projects.project_count, 0)                                               AS namespace_project_count,
      IFNULL(namespaces.is_current AND namespace_lineage.is_current, FALSE)           AS is_currently_valid
    FROM namespaces
    LEFT JOIN namespace_lineage
      ON namespaces.dim_namespace_id = namespace_lineage.namespace_id
      AND IFNULL(namespaces.parent_id, namespaces.dim_namespace_id) = IFNULL(namespace_lineage.parent_id, namespace_lineage.namespace_id)
    LEFT JOIN namespace_settings
      ON namespaces.dim_namespace_id = namespace_settings.namespace_id
    LEFT JOIN members
      ON namespaces.dim_namespace_id = members.source_id
    LEFT JOIN projects
      ON namespaces.dim_namespace_id = projects.namespace_id
    LEFT JOIN creators
      ON namespaces.dim_namespace_id = creators.group_id
    LEFT JOIN users
      ON creators.creator_id = users.dim_user_id
    LEFT JOIN map_namespace_internal
      ON namespace_lineage.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
    LEFT JOIN product_tiers saas_product_tiers
      ON saas_product_tiers.product_delivery_type = 'SaaS'
      AND namespace_lineage.ultimate_parent_plan_name = LOWER(IFF(saas_product_tiers.product_tier_name_short != 'Trial: Ultimate',
                                                                  saas_product_tiers.product_tier_historical_short,
                                                                  'ultimate_trial'))
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        namespaces.dim_namespace_id,
        namespaces.parent_id,
        namespace_lineage.ultimate_parent_id
      ORDER BY namespaces.namespace_updated_at DESC
    ) = 1

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@jpeguero",
    created_date="2021-01-14",
    updated_date="2022-02-22"
) }}
