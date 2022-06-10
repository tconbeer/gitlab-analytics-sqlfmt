{{ config(tags=["mnpi_exception"]) }}

with
    issues_and_epics as (

        select *
        from {{ ref("gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id") }}
        where link_type = 'Opportunity'

    ),
    notes as (

        select *
        from {{ ref("gitlab_dotcom_notes_linked_to_sfdc_account_id") }}
        where link_type = 'Opportunity'

    ),
    sfdc_opportunities as (select * from {{ ref("sfdc_opportunity_xf") }}),
    user_requests as (select * from {{ ref("gitlab_dotcom_gitlab_user_requests") }}),
    issues_epics_notes_unioned as (

        select distinct
            noteable_id, noteable_type, link_id as opportunity_id, sfdc_account_id
        from issues_and_epics

        UNION

        select distinct
            noteable_id, noteable_type, link_id as opportunity_id, sfdc_account_id
        from notes

    ),
    joined as (

        select
            user_requests.*,
            sfdc_opportunities.incremental_acv,
            sfdc_opportunities.opportunity_id as sfdc_opportunity_id,
            sfdc_opportunities.opportunity_name,
            sfdc_opportunities.sales_type,
            sfdc_opportunities.stage_name
        from issues_epics_notes_unioned
        inner join
            user_requests
            on issues_epics_notes_unioned.noteable_id = user_requests.noteable_id
            and issues_epics_notes_unioned.noteable_type = user_requests.noteable_type
            and issues_epics_notes_unioned.sfdc_account_id
            = user_requests.sfdc_account_id
        inner join
            sfdc_opportunities
            on issues_epics_notes_unioned.opportunity_id
            = sfdc_opportunities.opportunity_id

    )

select *
from joined
