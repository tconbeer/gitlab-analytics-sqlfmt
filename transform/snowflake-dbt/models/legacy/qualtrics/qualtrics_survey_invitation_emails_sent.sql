with
    qualtrics_distribution as (

        select *
        from {{ ref("qualtrics_distribution") }}
        qualify
            row_number() over (
                partition by distribution_id order by uploaded_at desc
            ) = 1

    ),
    email_sent_count as (

        select survey_id, sum(email_sent_count) as number_of_emails_sent

        from qualtrics_distribution
        group by survey_id

    )
select *
from email_sent_count
