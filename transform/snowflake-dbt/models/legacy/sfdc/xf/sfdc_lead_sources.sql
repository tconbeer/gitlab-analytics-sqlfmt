{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "legacy"}) }}

with
    sfdc_opportunity as (

        select distinct lead_source from {{ ref("sfdc_opportunity") }}

    ),
    sfdc_lead as (select distinct lead_source from {{ ref("sfdc_lead") }}),
    sfdc_contact as (select distinct lead_source from {{ ref("sfdc_contact") }}),
    base as (

        select *
        from sfdc_opportunity
        union all
        select *
        from sfdc_lead
        union all
        select *
        from sfdc_contact

    ),
    lead_sources as (

        select distinct lower(lead_source) as lead_source
        from base
        where lead_source is not null

    )

select
    row_number() OVER (order by lead_source) as lead_source_id,
    lead_source as initial_source,
    case
        when lead_source in ('advertisement')
        then 'Advertising'
        when lead_source like '%email%' or lead_source like '%newsletter%'
        then 'Email'
        when
            lead_source like '%event%'
            or lead_source like '%conference%'
            or lead_source like '%seminar%'
        then 'Events'
        when
            lead_source
            in (
                'Contact Request',
                'Enterprise Trial',
                'Development Request',
                'Prof Serv Request',
                'Web',
                'Webcast',
                'Web Chat',
                'Web Direct',
                'White Paper',
                'Training Request',
                'Consultancy Request',
                'Public Relations'
            )
        then 'Marketing Site'
        when
            lead_source
            in (
                'SDR Generated',
                'Linkedin',
                'LeadWare',
                'AE Generated',
                'Datanyze',
                'DiscoverOrg',
                'Clearbit'
            )
        then 'Prospecting'
        when
            lead_source
            in (
                'Gitorious',
                'GitLab Hosted',
                'GitLab EE instance',
                'GitLab.com',
                'CE Download',
                'CE Usage Ping'
            )
        then 'Product'
        when
            lead_source
            in (
                'Word of Mouth',
                'External Referral',
                'Employee Referral',
                'Partner',
                'Existing Client'
            )
        then 'Referral'
        else 'Other'
    end as initial_source_type
from lead_sources
