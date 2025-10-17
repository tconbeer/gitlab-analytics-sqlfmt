{% set column_name = "email_handle" %}

with
    source as (select * from {{ ref("gitlab_dotcom_emails_source") }}),
    intermediate as (

        select *, split_part(email_address, '@', 0) as email_handle from source

    ),
    filtered as (

        select *, {{ include_gitlab_email(column_name) }} as include_email_flg
        from intermediate
        where lower(email_address) like '%@gitlab.com'

    )

select *
from filtered
