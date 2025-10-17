{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "ephemeral"}) }}

with
    gitlab_issues as (

        select
            issue_id as noteable_id,
            'Issue' as noteable_type,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                issue_description,
                '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}'
            ) as sfdc_link_array,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                issue_description,
                '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}'
            ) as zendesk_link_array
        from {{ ref("gitlab_dotcom_issues_xf") }}
        where is_internal_issue and issue_description is not null

    ),
    gitlab_epics as (

        select
            epic_id as noteable_id,
            'Epic' as noteable_type,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                epic_description,
                '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}'
            ) as sfdc_link_array,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                epic_description, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}'
            ) as zendesk_link_array
        from {{ ref("gitlab_dotcom_epics_xf") }}
        where is_internal_epic and epic_description is not null

    ),
    gitlab_issues_and_epics as (

        select *
        from gitlab_issues

        union

        select *
        from gitlab_epics

    ),
    sfdc_accounts as (select * from {{ ref("sfdc_accounts_xf") }}),
    sfdc_contacts as (select * from {{ ref("sfdc_contact_xf") }}),
    sfdc_leads as (select * from {{ ref("sfdc_lead_xf") }}),
    sfdc_opportunities as (select * from {{ ref("sfdc_opportunity_xf") }}),
    zendesk_tickets as (select * from {{ ref("zendesk_tickets_xf") }}),
    gitlab_issues_and_epics_sfdc_id_flattened as (

        select
            noteable_id,
            noteable_type,
            "{{this.database}}".{{ target.schema }}.id15to18(
                f.value::varchar
            ) as sfdc_id_18char,
            substr(sfdc_id_18char, 0, 3) as sfdc_id_prefix
        from gitlab_issues_and_epics, table(flatten(sfdc_link_array)) f

    ),
    gitlab_issues_and_epics_zendesk_ticket_id_flattened as (

        select noteable_id, noteable_type, f.value as zendesk_ticket_id
        from gitlab_issues_and_epics, table(flatten(zendesk_link_array)) f

    ),
    gitlab_issues_and_epics_with_sfdc_objects as (

        select
            gitlab_issues_and_epics_sfdc_id_flattened.noteable_id,
            gitlab_issues_and_epics_sfdc_id_flattened.noteable_type,
            gitlab_issues_and_epics_sfdc_id_flattened.sfdc_id_18char as link_id,
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
            coalesce(
                sfdc_accounts.account_id,
                sfdc_contacts.account_id,
                sfdc_leads.converted_account_id,
                sfdc_opportunities.account_id
            ) as sfdc_account_id
        from gitlab_issues_and_epics_sfdc_id_flattened
        left join
            sfdc_accounts
            on gitlab_issues_and_epics_sfdc_id_flattened.sfdc_id_18char
            = sfdc_accounts.account_id
        left join
            sfdc_contacts
            on gitlab_issues_and_epics_sfdc_id_flattened.sfdc_id_18char
            = sfdc_contacts.contact_id
        left join
            sfdc_leads
            on gitlab_issues_and_epics_sfdc_id_flattened.sfdc_id_18char
            = sfdc_leads.lead_id
        left join
            sfdc_opportunities
            on gitlab_issues_and_epics_sfdc_id_flattened.sfdc_id_18char
            = sfdc_opportunities.opportunity_id
        where link_type is not null

    ),
    gitlab_issues_and_epics_with_zendesk_ticket as (

        select
            gitlab_issues_and_epics_zendesk_ticket_id_flattened.noteable_id,
            gitlab_issues_and_epics_zendesk_ticket_id_flattened.noteable_type,
            gitlab_issues_and_epics_zendesk_ticket_id_flattened.zendesk_ticket_id
            as link_id,
            'Zendesk Ticket' as link_type,
            zendesk_tickets.sfdc_account_id
        from gitlab_issues_and_epics_zendesk_ticket_id_flattened
        inner join
            zendesk_tickets
            on gitlab_issues_and_epics_zendesk_ticket_id_flattened.zendesk_ticket_id
            ::number
            = zendesk_tickets.ticket_id
        inner join
            sfdc_accounts on zendesk_tickets.sfdc_account_id = sfdc_accounts.account_id

    ),
    gitlab_issues_and_epics_with_sfdc_objects_union as (

        select *
        from gitlab_issues_and_epics_with_sfdc_objects

        union

        select *
        from gitlab_issues_and_epics_with_zendesk_ticket

    )

select *
from gitlab_issues_and_epics_with_sfdc_objects_union
