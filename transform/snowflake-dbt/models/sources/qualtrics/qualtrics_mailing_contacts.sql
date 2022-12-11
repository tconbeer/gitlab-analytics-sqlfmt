{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("qualtrics", "contact") }}),
    intermediate as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (

        select
            data_by_row['contactId']::varchar as contact_id,
            data_by_row['email']::varchar as contact_email,
            data_by_row['mailingListId']::varchar as mailing_list_id,
            data_by_row['unsubscribed']::boolean as is_unsubscribed,
            uploaded_at::timestamp as mailing_list_membership_observed_at
        from intermediate

    )
select *
from parsed
