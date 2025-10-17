{{ config(tags=["mnpi_exception"]) }}

with
    gitlab_dotcom_projects as (

        select * from {{ ref("gitlab_dotcom_projects_source") }}

    ),
    map_moved_duplicated_issue as (

        select * from {{ ref("map_moved_duplicated_issue") }}

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
                '(?<=gitlab.com\/)gitlab-org\/[^ ]*issues\/[0-9]{1,10}'
            ) as issue_links
        from collaboration_projects_with_ids
        where array_size(issue_links) != 0

    ),
    collaboration_projects_issue_descriptions_parsed as (

        select
            collaboration_projects_issue_descriptions.*,
            f.value as user_request_issue_path,
            replace(
                replace(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss'
            ) as user_request_issue_path_fixed,
            split_part(f.value, '/', -1)::number as user_request_issue_iid,
            rtrim(split_part(f.value, '/issues', 1), '/-') as user_request_project_path
        from collaboration_projects_issue_descriptions, table(flatten(issue_links)) f

    ),
    collaboration_projects_issue_notes as (

        select
            collaboration_projects_with_ids.*,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                issue_notes.note,
                '(?<=gitlab.com\/)gitlab-org\/[^ ]*issues\/[0-9]{1,10}'
            ) as issue_links,
            issue_notes.updated_at as note_updated_at
        from collaboration_projects_with_ids
        left join
            issue_notes
            on issue_notes.issue_id = collaboration_projects_with_ids.issue_id
        where array_size(issue_links) != 0

    ),
    collaboration_projects_issue_notes_parsed as (

        select
            collaboration_projects_issue_notes.*,
            f.value as user_request_issue_path,
            replace(
                replace(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss'
            ) as user_request_issue_path_fixed,
            split_part(f.value, '/', -1)::number as user_request_issue_iid,
            rtrim(split_part(f.value, '/issues', 1), '/-') as user_request_project_path
        from collaboration_projects_issue_notes, table(flatten(issue_links)) f

    ),
    collaboration_projects_issue_description_notes_unioned as (

        select
            account_id,
            gitlab_customer_success_project,
            collaboration_project_id,
            user_request_issue_iid,
            user_request_project_path,
            note_updated_at as link_last_updated_at
        from collaboration_projects_issue_notes_parsed

        union

        select
            account_id,
            gitlab_customer_success_project,
            collaboration_project_id,
            user_request_issue_iid,
            user_request_project_path,
            updated_at
        from collaboration_projects_issue_descriptions_parsed

    ),
    unioned_with_user_request_project_id as (

        select
            collaboration_projects_issue_description_notes_unioned.*,
            gitlab_dotcom_project_routes.project_id as user_request_project_id
        from collaboration_projects_issue_description_notes_unioned
        inner join
            gitlab_dotcom_project_routes
            on gitlab_dotcom_project_routes.path
            = collaboration_projects_issue_description_notes_unioned.user_request_project_path
        inner join
            gitlab_dotcom_projects
            on gitlab_dotcom_projects.project_id
            = gitlab_dotcom_project_routes.project_id

    ),
    unioned_with_issue_links as (

        select
            gitlab_issues.issue_id as dim_issue_id,
            unioned_with_user_request_project_id.account_id as dim_crm_account_id,
            unioned_with_user_request_project_id.collaboration_project_id
            as dim_collaboration_project_id,
            unioned_with_user_request_project_id.user_request_project_id
            as dim_project_id,
            unioned_with_user_request_project_id.gitlab_customer_success_project,
            unioned_with_user_request_project_id.user_request_issue_iid
            as issue_internal_id,
            unioned_with_user_request_project_id.link_last_updated_at
        from unioned_with_user_request_project_id
        inner join
            gitlab_issues
            on gitlab_issues.project_id
            = unioned_with_user_request_project_id.user_request_project_id
            and gitlab_issues.issue_iid
            = unioned_with_user_request_project_id.user_request_issue_iid

        union

        select
            gitlab_issues.issue_id as dim_issue_id,
            collaboration_projects_with_ids.account_id as dim_crm_account_id,
            collaboration_projects_with_ids.collaboration_project_id
            as dim_collaboration_project_id,
            gitlab_issues.project_id as dim_project_id,
            collaboration_projects_with_ids.gitlab_customer_success_project,
            gitlab_issues.issue_iid as issue_internal_id,
            issue_links.updated_at as link_last_updated_at
        from collaboration_projects_with_ids
        inner join
            issue_links
            on issue_links.source_id = collaboration_projects_with_ids.issue_id
        inner join gitlab_issues on gitlab_issues.issue_id = issue_links.target_id
        inner join
            gitlab_dotcom_project_routes
            on gitlab_dotcom_project_routes.project_id = gitlab_issues.project_id
        where gitlab_dotcom_project_routes.path like 'gitlab-org%'

    ),
    final as (  -- In case there are various issues that merge to the same, dedup them by taking the latest updated link

        select
            map_moved_duplicated_issue.dim_issue_id,
            unioned_with_issue_links.dim_crm_account_id,
            unioned_with_issue_links.dim_collaboration_project_id,
            unioned_with_issue_links.dim_project_id as dim_original_issue_project_id,
            unioned_with_issue_links.gitlab_customer_success_project,
            unioned_with_issue_links.issue_internal_id as original_issue_internal_id,
            unioned_with_issue_links.link_last_updated_at as link_last_updated_at
        from unioned_with_issue_links
        inner join
            map_moved_duplicated_issue
            on map_moved_duplicated_issue.issue_id
            = unioned_with_issue_links.dim_issue_id
        qualify
            row_number() over (
                partition by
                    map_moved_duplicated_issue.dim_issue_id,
                    unioned_with_issue_links.dim_crm_account_id
                order by unioned_with_issue_links.link_last_updated_at desc nulls last
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
