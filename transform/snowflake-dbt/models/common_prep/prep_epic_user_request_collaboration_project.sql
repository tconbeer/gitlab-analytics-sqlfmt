{{ config(tags=["mnpi_exception"]) }}

with
    gitlab_dotcom_namespaces as (

        select * from {{ ref("gitlab_dotcom_namespaces_source") }}

    ),
    issue_links as (

        select *
        from {{ ref("gitlab_dotcom_issue_links_source") }}
        where is_currently_valid

    ),
    issue_notes as (

        select noteable_id as issue_id, *
        from {{ ref("gitlab_dotcom_notes_source") }}
        where noteable_type = 'Issue' and system = false

    ),
    gitlab_epics as (

        select *
        from {{ ref("gitlab_dotcom_epics_source") }}
        qualify
            row_number() over (
                partition by group_id, epic_internal_id order by created_at desc
            )
            = 1

    ),
    gitlab_issues as (

        select *
        from {{ ref("gitlab_dotcom_issues_source") }}
        qualify
            row_number() over (
                partition by project_id, issue_iid order by created_at desc
            )
            = 1

    ),
    collaboration_projects as (

        select account_id, gitlab_customer_success_project
        from {{ ref("sfdc_account_source") }}
        where gitlab_customer_success_project like '%gitlab.com/%'

    ),
    gitlab_dotcom_project_routes as (

        select
            'https://gitlab.com/' || path as complete_path, source_id as project_id, *
        from {{ ref("gitlab_dotcom_routes_source") }}
        where source_type = 'Project'

    ),
    gitlab_dotcom_namespace_routes as (

        select
            'https://gitlab.com/' || path as complete_path, source_id as namespace_id, *
        from {{ ref("gitlab_dotcom_routes_source") }}
        where source_type = 'Namespace'

    ),
    collaboration_projects_with_ids as (

        select
            collaboration_projects.*,
            gitlab_dotcom_project_routes.project_id as collaboration_project_id,
            gitlab_issues.issue_id,
            gitlab_issues.issue_description,
            ifnull(
                gitlab_issues.issue_last_edited_at, gitlab_issues.created_at
            ) as updated_at
        from collaboration_projects
        left join
            gitlab_dotcom_project_routes
            on gitlab_dotcom_project_routes.complete_path
            = collaboration_projects.gitlab_customer_success_project
        left join
            gitlab_issues
            on gitlab_issues.project_id = gitlab_dotcom_project_routes.project_id

    ),
    collaboration_projects_issue_descriptions as (

        select
            *,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                issue_description,
                '(?<=gitlab.com\/groups\/)gitlab-org\/[^ ]*epics\/[0-9]{1,10}'
            ) as epic_links
        from collaboration_projects_with_ids
        where array_size(epic_links) != 0

    ),
    collaboration_projects_issue_descriptions_parsed as (

        select
            collaboration_projects_issue_descriptions.*,
            f.value as user_request_issue_path,
            replace(
                replace(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss'
            ) as user_request_epic_path_fixed,
            split_part(f.value, '/', -1)::number as user_request_epic_internal_id,
            rtrim(split_part(f.value, '/epics', 1), '/-') as user_request_namespace_path
        from collaboration_projects_issue_descriptions, table(flatten(epic_links)) f

    ),
    collaboration_projects_issue_notes as (

        select
            collaboration_projects_with_ids.*,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                issue_notes.note,
                '(?<=gitlab.com\/groups\/)gitlab-org\/[^ ]*epics\/[0-9]{1,10}'
            ) as epic_links,
            issue_notes.updated_at as note_updated_at
        from collaboration_projects_with_ids
        left join
            issue_notes
            on issue_notes.issue_id = collaboration_projects_with_ids.issue_id
        where array_size(epic_links) != 0

    ),
    collaboration_projects_issue_notes_parsed as (

        select
            collaboration_projects_issue_notes.*,
            f.value as user_request_epic_path,
            replace(
                replace(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss'
            ) as user_request_epic_path_fixed,
            split_part(f.value, '/', -1)::number as user_request_epic_internal_id,
            rtrim(split_part(f.value, '/epics', 1), '/-') as user_request_namespace_path
        from collaboration_projects_issue_notes, table(flatten(epic_links)) f

    ),
    collaboration_projects_issue_description_notes_unioned as (

        select
            account_id,
            gitlab_customer_success_project,
            collaboration_project_id,
            user_request_epic_internal_id,
            user_request_namespace_path,
            note_updated_at as link_last_updated_at
        from collaboration_projects_issue_notes_parsed

        union

        select
            account_id,
            gitlab_customer_success_project,
            collaboration_project_id,
            user_request_epic_internal_id,
            user_request_namespace_path,
            updated_at
        from collaboration_projects_issue_descriptions_parsed

    ),
    unioned_with_user_request_namespace_id as (

        select
            collaboration_projects_issue_description_notes_unioned.*,
            gitlab_dotcom_namespace_routes.namespace_id as user_request_namespace_id
        from collaboration_projects_issue_description_notes_unioned
        inner join
            gitlab_dotcom_namespace_routes
            on gitlab_dotcom_namespace_routes.path
            = collaboration_projects_issue_description_notes_unioned.user_request_namespace_path
        inner join
            gitlab_dotcom_namespaces
            on gitlab_dotcom_namespaces.namespace_id
            = gitlab_dotcom_namespace_routes.namespace_id

    ),
    final
    -- In case there are various issues with the same link to issues and
    -- dim_crm_account_id, dedup them by taking the latest updated link
    as (

        select
            gitlab_epics.epic_id as dim_epic_id,
            unioned_with_user_request_namespace_id.account_id as dim_crm_account_id,
            unioned_with_user_request_namespace_id.collaboration_project_id
            as dim_collaboration_project_id,
            unioned_with_user_request_namespace_id.user_request_namespace_id
            as dim_namespace_id,
            unioned_with_user_request_namespace_id.gitlab_customer_success_project
            as gitlab_customer_success_project,
            unioned_with_user_request_namespace_id.user_request_epic_internal_id
            as epic_internal_id,
            unioned_with_user_request_namespace_id.link_last_updated_at
            as link_last_updated_at
        from unioned_with_user_request_namespace_id
        inner join
            gitlab_epics
            on gitlab_epics.group_id
            = unioned_with_user_request_namespace_id.user_request_namespace_id
            and gitlab_epics.epic_internal_id
            = unioned_with_user_request_namespace_id.user_request_epic_internal_id
        qualify
            row_number() over (
                partition by
                    gitlab_epics.epic_id,
                    unioned_with_user_request_namespace_id.account_id
                order by
                    unioned_with_user_request_namespace_id.link_last_updated_at desc nulls last
            )
            = 1

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-10-12",
            updated_date="2022-01-10",
        )
    }}
