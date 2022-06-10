{{ config(tags=["mnpi_exception"]) }}

with
    epic_issues as (select * from {{ ref("gitlab_dotcom_epic_issues") }}),
    epics as (select * from {{ ref("gitlab_dotcom_epics_xf") }}),
    gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id as (

        select *
        from {{ ref("gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id") }}

    ),
    gitlab_dotcom_notes_linked_to_sfdc_account_id as (

        select * from {{ ref("gitlab_dotcom_notes_linked_to_sfdc_account_id") }}

    ),
    issues as (select * from {{ ref("gitlab_dotcom_issues_xf") }}),
    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces_xf") }}),
    sfdc_accounts as (select * from {{ ref("sfdc_accounts_xf") }}),
    /* Created 4 Separate CTEs to be unioned */
    sfdc_opportunities as (select * from {{ ref("sfdc_opportunity_xf") }}),
    sfdc_accounts_from_issue_notes as (

        select distinct
            'Issue' as noteable_type,
            'Note' as mention_type,
            issues.issue_id as noteable_id,
            issues.issue_iid as noteable_iid,
            issues.issue_title as noteable_title,
            issues.issue_created_at as noteable_created_at,
            issues.milestone_id,
            issues.state as noteable_state,
            issues.weight,
            issues.labels,
            projects.project_name,
            projects.project_id,
            namespaces.namespace_id,
            namespaces.namespace_name,
            sfdc_accounts.account_id as sfdc_account_id,
            sfdc_accounts.account_type as sfdc_account_type,
            sfdc_accounts.carr_this_account,
            sfdc_accounts.carr_account_family,
            epics.epic_title
        from gitlab_dotcom_notes_linked_to_sfdc_account_id
        inner join
            issues
            on gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id
            = issues.issue_id
        left join projects on issues.project_id = projects.project_id
        left join namespaces on projects.namespace_id = namespaces.namespace_id
        left join
            sfdc_accounts
            on gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id
            = sfdc_accounts.account_id
        left join epic_issues on issues.issue_id = epic_issues.issue_id
        left join epics on epic_issues.epic_id = epics.epic_id
        where gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Issue'

    ),
    sfdc_accounts_from_epic_notes as (

        select distinct
            'Epic' as noteable_type,
            'Note' as mention_type,
            epics.epic_id as noteable_id,
            epics.epic_internal_id as noteable_iid,
            epics.epic_title as noteable_title,
            epics.created_at as noteable_created_at,
            null as milestone_id,
            epics.state as epic_state,
            null as weight,
            epics.labels as labels,
            null as project_name,
            null as project_id,
            namespaces.namespace_id,
            namespaces.namespace_name,
            sfdc_accounts.account_id as sfdc_account_id,
            sfdc_accounts.account_type as sfdc_account_type,
            sfdc_accounts.carr_this_account,
            sfdc_accounts.carr_account_family,
            epics.epic_title  -- Redundant in this case.
        from gitlab_dotcom_notes_linked_to_sfdc_account_id
        inner join
            epics
            on gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = epics.epic_id
        left join namespaces on epics.group_id = namespaces.namespace_id
        left join
            sfdc_accounts
            on gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id
            = sfdc_accounts.account_id
        where gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Epic'

    ),
    sfdc_accounts_from_issue_descriptions as (

        select distinct
            'Issue' as noteable_type,
            'Description' as mention_type,
            issues.issue_id,
            issues.issue_iid,
            issues.issue_title,
            issues.issue_created_at,
            issues.milestone_id,
            issues.state as issue_state,
            issues.weight,
            issues.labels,
            projects.project_name,
            projects.project_id,
            namespaces.namespace_id,
            namespaces.namespace_name,
            sfdc_accounts.account_id as sfdc_account_id,
            sfdc_accounts.account_type as sfdc_account_type,
            sfdc_accounts.carr_this_account,
            sfdc_accounts.carr_account_family,
            epics.epic_title
        from gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id
        inner join
            issues
            on gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_id
            = issues.issue_id
            and gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_type
            = 'Issue'
        left join projects on issues.project_id = projects.project_id
        left join namespaces on projects.namespace_id = namespaces.namespace_id
        left join
            sfdc_accounts
            on gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.sfdc_account_id
            = sfdc_accounts.account_id
        left join epic_issues on issues.issue_id = epic_issues.issue_id
        left join epics on epic_issues.epic_id = epics.epic_id

    ),
    sfdc_accounts_from_epic_descriptions as (

        select distinct
            'Epic' as noteable_type,
            'Description' as mention_type,
            epics.epic_id as noteable_id,
            epics.epic_internal_id as noteable_iid,
            epics.epic_title as noteable_title,
            epics.created_at as noteable_created_at,
            null as milestone_id,
            epics.state as epic_state,
            null as weight,
            epics.labels as labels,
            null as project_name,
            null as project_id,
            namespaces.namespace_id,
            namespaces.namespace_name,
            sfdc_accounts.account_id as sfdc_account_id,
            sfdc_accounts.account_type as sfdc_account_type,
            sfdc_accounts.carr_this_account,
            sfdc_accounts.carr_account_family,
            epics.epic_title  -- Redundant in this case.
        from gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id
        inner join
            epics
            on gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_id
            = epics.epic_id
            and gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_type
            = 'Epic'
        left join namespaces on epics.group_id = namespaces.namespace_id
        left join
            sfdc_accounts
            on gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.sfdc_account_id
            = sfdc_accounts.account_id

    ),
    unioned as (

        /* Notes */
        select *
        from sfdc_accounts_from_issue_notes

        UNION

        select *
        from sfdc_accounts_from_epic_notes

        /* Descriptions */
        UNION

        select *
        from sfdc_accounts_from_issue_descriptions

        UNION

        select *
        from sfdc_accounts_from_epic_descriptions

    )

select *
from unioned
where sfdc_account_id is not null
