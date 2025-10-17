with
    source as (

        select *
        from {{ source("greenhouse", "interviewers") }}
        -- intentionally excluded record, details in
        -- https://gitlab.com/gitlab-data/analytics/-/issues/10211
        -- hide PII columns, just exclude one record intentionally
        where
            not (user is null and scorecard_id is null and interview_id = 101187575002)

    ),
    renamed as (select distinct user as interviewer_name from source)

select *
from renamed
