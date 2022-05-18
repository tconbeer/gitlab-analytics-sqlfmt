{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("issue", "gitlab_dotcom_issues_source"),
            ("map_namespace_internal", "map_namespace_internal"),
            ("map_namespace_lineage", "map_namespace_lineage"),
            ("project", "gitlab_dotcom_projects_source"),
            ("zendesk_ticket", "zendesk_tickets_source"),
            ("zendesk_organization", "zendesk_organizations_source"),
            ("map_moved_duplicated_issue", "map_moved_duplicated_issue"),
            ("sfdc_opportunity_source", "sfdc_opportunity_source"),
        ]
    )
}}

,
issue_notes as (

    select noteable_id as issue_id, *
    from {{ ref("gitlab_dotcom_notes_source") }}
    where noteable_type = 'Issue' and system = false

),
issue_extended as (

    select map_namespace_lineage.dim_namespace_ultimate_parent_id, issue.*
    from issue
    inner join project on project.project_id = issue.project_id
    inner join
        map_namespace_lineage
        on project.namespace_id = map_namespace_lineage.dim_namespace_id
    -- Gitlab-org group namespace id
    where map_namespace_lineage.dim_namespace_ultimate_parent_id = 9970

),
gitlab_issue_description_parsing as (

    select
        issue_id,
        "{{this.database}}".{{ target.schema }}.regexp_to_array(
            issue_description,
            '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}'
        ) as sfdc_link_array,
        "{{this.database}}".{{ target.schema }}.regexp_to_array(
            issue_description, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}'
        ) as zendesk_link_array,
        split_part(
            regexp_substr(issue_description, '~"customer priority::[0-9]{1,2}'),
            '::',
            -1
        )::number as request_priority,
        ifnull(issue_last_edited_at, created_at) as issue_last_edited_at
    from issue_extended
    where
        issue_description is not null and not (
            array_size(sfdc_link_array) = 0 and array_size(zendesk_link_array) = 0
        )

),
issue_notes_extended as (

    select issue_notes.*
    from issue_notes
    inner join issue_extended on issue_notes.issue_id = issue_extended.issue_id

),
gitlab_issue_notes_parsing as (

    select
        note_id,
        issue_id,
        "{{this.database}}".{{ target.schema }}.regexp_to_array(
            note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}'
        ) as sfdc_link_array,
        "{{this.database}}".{{ target.schema }}.regexp_to_array(
            note, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}'
        ) as zendesk_link_array,
        split_part(
            regexp_substr(note, '~"customer priority::[0-9]{1,2}'), '::', -1
        )::number as request_priority,
        created_at as note_created_at,
        updated_at as note_updated_at
    from issue_notes_extended
    where not (array_size(sfdc_link_array) = 0 and array_size(zendesk_link_array) = 0)

),
gitlab_issue_notes_sfdc_links as (

    select
        note_id,
        issue_id,
        "{{this.database}}".{{ target.schema }}.id15to18(
            f.value::varchar
        ) as sfdc_id_18char,
        substr(sfdc_id_18char, 0, 3) as sfdc_id_prefix,
        case
            when sfdc_id_prefix = '001'
            then 'Account'
            when sfdc_id_prefix = '003'
            then 'Contact'
            when sfdc_id_prefix = '00Q'
            then 'Lead'
            when sfdc_id_prefix = '006'
            then 'Opportunity'
            else null
        end as link_type,
        iff(link_type = 'Account', sfdc_id_18char, null) as dim_crm_account_id,
        iff(link_type = 'Opportunity', sfdc_id_18char, null) as dim_crm_opportunity_id,
        request_priority,
        note_created_at,
        note_updated_at
    from gitlab_issue_notes_parsing, table(flatten(sfdc_link_array)) f
    where link_type in ('Account', 'Opportunity')

),
gitlab_issue_notes_sfdc_links_with_account as (

    select
        gitlab_issue_notes_sfdc_links.issue_id,
        gitlab_issue_notes_sfdc_links.sfdc_id_18char,
        gitlab_issue_notes_sfdc_links.sfdc_id_prefix,
        gitlab_issue_notes_sfdc_links.link_type,
        ifnull(
            gitlab_issue_notes_sfdc_links.dim_crm_account_id,
            sfdc_opportunity_source.account_id
        ) as dim_crm_account_id,
        gitlab_issue_notes_sfdc_links.dim_crm_opportunity_id,
        gitlab_issue_notes_sfdc_links.request_priority,
        gitlab_issue_notes_sfdc_links.note_created_at,
        gitlab_issue_notes_sfdc_links.note_updated_at
    from gitlab_issue_notes_sfdc_links
    left join
        sfdc_opportunity_source
        on sfdc_opportunity_source.opportunity_id
        = gitlab_issue_notes_sfdc_links.dim_crm_opportunity_id
    where
        ifnull(
            gitlab_issue_notes_sfdc_links.dim_crm_account_id,
            sfdc_opportunity_source.account_id
        ) is not null

),
gitlab_issue_description_sfdc_links as (

    select
        issue_id,
        "{{this.database}}".{{ target.schema }}.id15to18(
            f.value::varchar
        ) as sfdc_id_18char,
        substr(sfdc_id_18char, 0, 3) as sfdc_id_prefix,
        case
            when sfdc_id_prefix = '001'
            then 'Account'
            when sfdc_id_prefix = '003'
            then 'Contact'
            when sfdc_id_prefix = '00Q'
            then 'Lead'
            when sfdc_id_prefix = '006'
            then 'Opportunity'
            else null
        end as link_type,
        iff(link_type = 'Account', sfdc_id_18char, null) as dim_crm_account_id,
        iff(link_type = 'Opportunity', sfdc_id_18char, null) as dim_crm_opportunity_id,
        request_priority,
        issue_last_edited_at
    from gitlab_issue_description_parsing, table(flatten(sfdc_link_array)) f
    where link_type in ('Account', 'Opportunity')

),
gitlab_issue_description_sfdc_links_with_account as (

    select
        gitlab_issue_description_sfdc_links.issue_id,
        gitlab_issue_description_sfdc_links.sfdc_id_18char,
        gitlab_issue_description_sfdc_links.sfdc_id_prefix,
        gitlab_issue_description_sfdc_links.link_type,
        ifnull(
            gitlab_issue_description_sfdc_links.dim_crm_account_id,
            sfdc_opportunity_source.account_id
        ) as dim_crm_account_id,
        gitlab_issue_description_sfdc_links.dim_crm_opportunity_id,
        gitlab_issue_description_sfdc_links.request_priority,
        gitlab_issue_description_sfdc_links.issue_last_edited_at
    from gitlab_issue_description_sfdc_links
    left join
        sfdc_opportunity_source
        on sfdc_opportunity_source.opportunity_id
        = gitlab_issue_description_sfdc_links.dim_crm_opportunity_id
    where
        ifnull(
            gitlab_issue_description_sfdc_links.dim_crm_account_id,
            sfdc_opportunity_source.account_id
        ) is not null

),
gitlab_issue_notes_zendesk_link as (

    select
        note_id,
        issue_id,
        replace(f.value, '"', '') as dim_ticket_id,
        'Zendesk Ticket' as link_type,
        request_priority,
        note_created_at,
        note_updated_at
    from gitlab_issue_notes_parsing, table(flatten(zendesk_link_array)) f

),
gitlab_issue_notes_zendesk_with_sfdc_account as (

    select
        gitlab_issue_notes_zendesk_link.*,
        zendesk_organization.sfdc_account_id as dim_crm_account_id
    from gitlab_issue_notes_zendesk_link
    left join
        zendesk_ticket
        on zendesk_ticket.ticket_id = gitlab_issue_notes_zendesk_link.dim_ticket_id
    left join
        zendesk_organization
        on zendesk_organization.organization_id = zendesk_ticket.organization_id
    where zendesk_organization.sfdc_account_id is not null

),
gitlab_issue_description_zendesk_link as (

    select
        issue_id,
        replace(f.value, '"', '') as dim_ticket_id,
        'Zendesk Ticket' as link_type,
        request_priority,
        issue_last_edited_at
    from gitlab_issue_description_parsing, table(flatten(zendesk_link_array)) f

),
gitlab_issue_description_zendesk_with_sfdc_account as (

    select
        gitlab_issue_description_zendesk_link.*,
        zendesk_organization.sfdc_account_id as dim_crm_account_id
    from gitlab_issue_description_zendesk_link
    left join
        zendesk_ticket
        on zendesk_ticket.ticket_id
        = gitlab_issue_description_zendesk_link.dim_ticket_id
    left join
        zendesk_organization
        on zendesk_organization.organization_id = zendesk_ticket.organization_id
    where zendesk_organization.sfdc_account_id is not null

),
union_links as (

    select
        issue_id as dim_issue_id,
        link_type,
        dim_crm_opportunity_id,
        dim_crm_account_id,
        null as dim_ticket_id,
        iff(request_priority is null, true, false) as is_request_priority_empty,
        ifnull(request_priority, 1)::number as request_priority,
        note_updated_at as link_last_updated_at
    from gitlab_issue_notes_sfdc_links_with_account
    qualify
        row_number() over (
            partition by issue_id, sfdc_id_18char order by note_updated_at desc
        ) = 1

    union

    select
        issue_id,
        link_type,
        null dim_crm_opportunity_id,
        dim_crm_account_id,
        dim_ticket_id,
        iff(request_priority is null, true, false) as is_request_priority_empty,
        ifnull(request_priority, 1)::number as request_priority,
        note_updated_at
    from gitlab_issue_notes_zendesk_with_sfdc_account
    qualify
        row_number() over (
            partition by issue_id, dim_ticket_id order by note_updated_at desc
        ) = 1

    union

    select
        gitlab_issue_description_sfdc_links_with_account.issue_id,
        gitlab_issue_description_sfdc_links_with_account.link_type,
        gitlab_issue_description_sfdc_links_with_account.dim_crm_opportunity_id,
        gitlab_issue_description_sfdc_links_with_account.dim_crm_account_id,
        null as dim_ticket_id,
        iff(
            gitlab_issue_description_sfdc_links_with_account.request_priority is null,
            true,
            false
        ) as is_request_priority_empty,
        ifnull(
            gitlab_issue_description_sfdc_links_with_account.request_priority, 1
        )::number as request_priority,
        gitlab_issue_description_sfdc_links_with_account.issue_last_edited_at
    from gitlab_issue_description_sfdc_links_with_account
    left join
        gitlab_issue_notes_sfdc_links
        on gitlab_issue_description_sfdc_links_with_account.issue_id
        = gitlab_issue_notes_sfdc_links.issue_id
        and gitlab_issue_description_sfdc_links_with_account.sfdc_id_18char
        = gitlab_issue_notes_sfdc_links.sfdc_id_18char
    where gitlab_issue_notes_sfdc_links.issue_id is null

    union

    select
        gitlab_issue_description_zendesk_with_sfdc_account.issue_id,
        gitlab_issue_description_zendesk_with_sfdc_account.link_type,
        null dim_crm_opportunity_id,
        gitlab_issue_description_zendesk_with_sfdc_account.dim_crm_account_id,
        gitlab_issue_description_zendesk_with_sfdc_account.dim_ticket_id,
        iff(
            gitlab_issue_description_zendesk_with_sfdc_account.request_priority is null,
            true,
            false
        ) as is_request_priority_empty,
        ifnull(
            gitlab_issue_description_zendesk_with_sfdc_account.request_priority, 1
        )::number as request_priority,
        gitlab_issue_description_zendesk_with_sfdc_account.issue_last_edited_at
    from gitlab_issue_description_zendesk_with_sfdc_account
    left join
        gitlab_issue_notes_zendesk_link
        on gitlab_issue_description_zendesk_with_sfdc_account.issue_id
        = gitlab_issue_notes_zendesk_link.issue_id
        and gitlab_issue_description_zendesk_with_sfdc_account.dim_ticket_id
        = gitlab_issue_notes_zendesk_link.dim_ticket_id
    where gitlab_issue_notes_zendesk_link.issue_id is null

),
union_links_mapped_issues as (

    select
        map_moved_duplicated_issue.dim_issue_id,
        union_links.link_type,
        {{ get_keyed_nulls("union_links.dim_crm_opportunity_id") }}
        as dim_crm_opportunity_id,
        union_links.dim_crm_account_id,
        ifnull(union_links.dim_ticket_id, -1)::number as dim_ticket_id,
        union_links.request_priority,
        union_links.is_request_priority_empty,
        union_links.link_last_updated_at
    from union_links
    inner join
        map_moved_duplicated_issue
        on map_moved_duplicated_issue.issue_id = union_links.dim_issue_id

),
final as (

    -- Take the latest update of the issue||SFDC/Zendesk link combination.
    -- This could happen if a issue link combination appears in an issue that was
    -- moved/duplicated
    -- to other and in that other issue the same link is also posted.
    -- And those links could have different priorities 
    select
        dim_issue_id,
        link_type,
        dim_crm_opportunity_id,
        dim_crm_account_id,
        dim_ticket_id,
        request_priority,
        is_request_priority_empty,
        link_last_updated_at
    from union_links_mapped_issues
    qualify
        row_number() over (
            partition by
                dim_issue_id, dim_crm_opportunity_id, dim_crm_account_id, dim_ticket_id
            order by link_last_updated_at desc
        ) = 1

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
