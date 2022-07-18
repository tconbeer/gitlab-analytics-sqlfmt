{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}
with
    qualtrics_mailing_contacts as (

        select * from {{ ref("qualtrics_mailing_contacts") }}

    ),
    qualtrics_distribution as (select * from {{ ref("qualtrics_distribution_xf") }}),
    qualtrics_survey as (select * from {{ ref("qualtrics_survey") }}),
    mailing_list_distinct_versions as (

        select distinct mailing_list_id, mailing_list_membership_observed_at
        from qualtrics_mailing_contacts

    ),
    distribution_mailing_list_version as (

        select
            dist.mailing_list_id as mailing_list_id,
            dist.distribution_id as distribution_id,
            dist.survey_id as survey_id,
            dist.mailing_sent_at as mailing_sent_at,
            min(
                mailing_list_membership_observed_at
            ) as mailing_list_membership_observed_at
        from qualtrics_distribution dist
        inner join
            mailing_list_distinct_versions ml
            on dist.mailing_sent_at < ml.mailing_list_membership_observed_at
            {{ dbt_utils.group_by(n=4) }}

    ),
    distribution_contacts_joined as (

        select
            m.contact_email as contact_email,
            d.mailing_sent_at as mailing_sent_at,
            s.survey_name as survey_name
        from distribution_mailing_list_version d
        inner join
            qualtrics_mailing_contacts m
            on d.mailing_list_membership_observed_at
            = m.mailing_list_membership_observed_at
            and not m.is_unsubscribed
        inner join qualtrics_survey s on d.survey_id = s.survey_id

    )

select *
from distribution_contacts_joined
