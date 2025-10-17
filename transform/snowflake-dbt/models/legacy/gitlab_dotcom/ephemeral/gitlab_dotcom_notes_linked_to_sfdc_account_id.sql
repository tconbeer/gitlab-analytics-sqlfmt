{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "ephemeral"}) }}

with
    gitlab_notes as (select * from {{ ref("gitlab_dotcom_internal_notes_xf") }}),
    sfdc_accounts as (select * from {{ ref("sfdc_accounts_xf") }}),
    sfdc_contacts as (select * from {{ ref("sfdc_contact_xf") }}),
    sfdc_leads as (select * from {{ ref("sfdc_lead_xf") }}),
    sfdc_opportunities as (select * from {{ ref("sfdc_opportunity_xf") }}),
    zendesk_tickets as (select * from {{ ref("zendesk_tickets_xf") }}),
    gitlab_notes_sfdc_id_flattened as (

        select
            gitlab_notes.note_id,
            noteable_id,
            noteable_type,
            "{{this.database}}".{{ target.schema }}.id15to18(
                f.value::varchar
            ) as sfdc_id_18char,
            substr(sfdc_id_18char, 0, 3) as sfdc_id_prefix
        from gitlab_notes, table(flatten(sfdc_link_array)) f

    ),
    gitlab_notes_zendesk_ticket_id_flattened as (

        select
            gitlab_notes.note_id,
            noteable_id,
            noteable_type,
            f.value as zendesk_ticket_id
        from gitlab_notes, table(flatten(zendesk_link_array)) f

    ),
    gitlab_notes_with_sfdc_objects as (

        select
            gitlab_notes_sfdc_id_flattened.noteable_id,
            gitlab_notes_sfdc_id_flattened.noteable_type,
            gitlab_notes_sfdc_id_flattened.sfdc_id_18char as link_id,
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
        from gitlab_notes_sfdc_id_flattened
        left join
            sfdc_accounts
            on gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_accounts.account_id
        left join
            sfdc_contacts
            on gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_contacts.contact_id
        left join
            sfdc_leads
            on gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_leads.lead_id
        left join
            sfdc_opportunities
            on gitlab_notes_sfdc_id_flattened.sfdc_id_18char
            = sfdc_opportunities.opportunity_id
        where link_type is not null

    ),
    gitlab_notes_with_zendesk_ticket as (

        select
            gitlab_notes_zendesk_ticket_id_flattened.noteable_id,
            gitlab_notes_zendesk_ticket_id_flattened.noteable_type,
            gitlab_notes_zendesk_ticket_id_flattened.zendesk_ticket_id as link_id,
            'Zendesk Ticket' as link_type,
            zendesk_tickets.sfdc_account_id
        from gitlab_notes_zendesk_ticket_id_flattened
        inner join
            zendesk_tickets
            on gitlab_notes_zendesk_ticket_id_flattened.zendesk_ticket_id::number
            = zendesk_tickets.ticket_id
        inner join
            sfdc_accounts on zendesk_tickets.sfdc_account_id = sfdc_accounts.account_id

    ),
    gitlab_notes_with_sfdc_objects_union as (

        select *
        from gitlab_notes_with_sfdc_objects

        union

        select *
        from gitlab_notes_with_zendesk_ticket

    )

select *
from gitlab_notes_with_sfdc_objects_union
