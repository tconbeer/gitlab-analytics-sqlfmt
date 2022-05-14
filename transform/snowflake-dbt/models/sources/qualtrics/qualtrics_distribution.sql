with
    source as (select * from {{ source("qualtrics", "distribution") }}),
    intermediate as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (

        select
            data_by_row['recipients'] ['mailingListId']::varchar as mailing_list_id,
            data_by_row['id']::varchar as distribution_id,
            data_by_row['surveyLink'] ['surveyId']::varchar as survey_id,
            data_by_row['sendDate']::timestamp as mailing_sent_at,
            data_by_row['stats'] ['blocked']::number as email_blocked_count,
            data_by_row['stats'] ['bounced']::number as email_bounced_count,
            data_by_row['stats'] ['complaints']::number as complaint_count,
            data_by_row['stats'] ['failed']::number as email_failed_count,
            data_by_row['stats'] ['finished']::number as survey_finished_count,
            data_by_row['stats'] ['opened']::number as email_opened_count,
            data_by_row['stats'] ['sent']::number as email_sent_count,
            data_by_row['stats'] ['skipped']::number as email_skipped_count,
            data_by_row['stats'] ['started']::number as survey_started_count,
            uploaded_at::timestamp as uploaded_at
        from intermediate

    )

select *
from parsed
