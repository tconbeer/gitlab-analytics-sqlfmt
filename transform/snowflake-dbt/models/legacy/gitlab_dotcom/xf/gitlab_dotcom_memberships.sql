with  -- direct group and project members
    members as (

        select *
        from {{ ref("gitlab_dotcom_members") }} members
        where
            is_currently_valid = true
            and user_id is not null
            and {{ filter_out_blocked_users("members", "user_id") }}
        qualify
            rank() over (
                partition by user_id, source_id, member_source_type
                order by access_level desc, invite_created_at desc
            )
            = 1

    ),
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    users as (select * from {{ ref("gitlab_dotcom_users") }}),
    -- groups invited to groups
    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),
    group_group_links as (

        select *
        from {{ ref("gitlab_dotcom_group_group_links") }}
        where is_currently_valid = true

    ),  -- groups invited to projects
    project_group_links as (

        select *
        from {{ ref("gitlab_dotcom_project_group_links") }}
        where is_currently_valid = true

    ),
    group_group_links_lineage as (

        select
            group_group_links.shared_group_id,  -- the "host" group
            group_group_links.group_group_link_id,
            group_group_links.shared_with_group_id,  -- the "guest" group
            group_group_links.group_access,
            -- all parent namespaces for the "guest" group
            namespace_lineage.upstream_lineage as base_and_ancestors
        from group_group_links
        inner join
            namespace_lineage
            on group_group_links.shared_with_group_id = namespace_lineage.namespace_id

    ),
    project_group_links_lineage as (

        select
            -- the "host" group the project directly belongs to
            projects.namespace_id as shared_group_id,
            project_group_links.project_group_link_id,
            project_group_links.group_id as shared_with_group_id,  -- the "guest" group
            project_group_links.group_access,
            -- all parent namespaces for the "guest" group
            namespace_lineage.upstream_lineage as base_and_ancestors
        from project_group_links
        inner join projects on project_group_links.project_id = projects.project_id
        inner join
            namespace_lineage
            on project_group_links.group_id = namespace_lineage.namespace_id

    ),
    group_group_links_flattened as (

        -- creates one row for each "guest" group and its parent namespaces
        select group_group_links_lineage.*, f.value as shared_with_group_lineage
        from
            group_group_links_lineage,
            table(flatten(group_group_links_lineage.base_and_ancestors)) f

    ),
    project_group_links_flattened as (

        -- creates one row for each "guest" group and its parent namespaces
        select project_group_links_lineage.*, f.value as shared_with_group_lineage
        from
            project_group_links_lineage,
            table(flatten(project_group_links_lineage.base_and_ancestors)) f

    ),
    group_members as (select * from members where member_source_type = 'Namespace'),
    project_members as (

        select projects.namespace_id, members.*
        from members
        inner join projects on members.source_id = projects.project_id
        where member_source_type = 'Project'

    ),
    group_group_link_members as (

        select *
        from group_group_links_flattened
        inner join
            group_members
            on group_group_links_flattened.shared_with_group_lineage
            = group_members.source_id

    ),
    project_group_link_members as (

        select *
        from project_group_links_flattened
        inner join
            group_members
            on project_group_links_flattened.shared_with_group_lineage
            = group_members.source_id

    ),
    individual_namespaces as (select * from namespaces where namespace_type = 'User'),
    unioned as (

        select
            source_id as namespace_id,
            'group_membership' as membership_source_type,
            source_id as membership_source_id,
            access_level,
            null as group_access,  -- direct member of group
            requested_at,
            user_id
        from group_members

        union

        select
            namespace_id,
            'project_membership' as membership_source_type,
            source_id as membership_source_id,
            access_level,
            null as group_access,  -- direct member of project
            requested_at,
            user_id
        from project_members

        union

        select
            shared_group_id as namespace_id,
            iff(
                shared_with_group_lineage = shared_with_group_id,
                'group_group_link',
                'group_group_link_ancestor'
            -- differentiate "guest" group from its parent namespaces
            ) as membership_source_type,
            group_group_link_id as membership_source_id,
            access_level,
            group_access,
            requested_at,
            user_id
        from group_group_link_members

        union

        select
            shared_group_id as namespace_id,
            iff(
                shared_with_group_lineage = shared_with_group_id,
                'project_group_link',
                'project_group_link_ancestor'
            -- differentiate "guest" group from its parent namespaces
            ) as membership_source_type,
            project_group_link_id as membership_source_id,
            access_level,
            group_access,
            requested_at,
            user_id
        from project_group_link_members

        union

        select
            namespace_id,
            'individual_namespace' as membership_source_type,
            namespace_id as membership_source_id,
            50 as access_level,  -- implied by ownership
            null as group_access,  -- implied by ownership
            null as requested_at,  -- implied by ownership
            owner_id as user_id
        from individual_namespaces

    ),
    joined as (

        select
            namespace_lineage.ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id,
            namespace_lineage.ultimate_parent_plan_title,
            unioned.*,
            users.state as user_state,
            users.user_type
        from unioned
        inner join
            namespace_lineage on unioned.namespace_id = namespace_lineage.namespace_id
        inner join users on unioned.user_id = users.user_id

    ),
    final as (

        select
            ultimate_parent_id,
            ultimate_parent_plan_id,
            ultimate_parent_plan_title,
            namespace_id,
            membership_source_type,
            membership_source_id,
            access_level,
            group_access,
            requested_at,
            user_id,
            user_state,
            user_type,
            -- exclude any user with guest access
            iff(access_level = 10 or group_access = 10, true, false) as is_guest,
            iff(
                user_state = 'active'
                and (user_type != 6 or user_type is null)
                and requested_at is null,
                true,  -- must be active, not a project bot, and not awaiting access
                false
            ) as is_active,
            iff(
                (
                    ultimate_parent_plan_title = 'gold'
                    and is_active = true
                    and is_guest = false
                )
                or (ultimate_parent_plan_title != 'gold' and is_active = true),
                true,  -- exclude guests if namespace has gold plan
                false
            ) as is_billable
        from joined

    )

select *
from final
