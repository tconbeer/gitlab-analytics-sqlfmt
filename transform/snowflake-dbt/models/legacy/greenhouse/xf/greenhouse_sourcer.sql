with
    applications as (select * from {{ ref("greenhouse_applications_source") }}),
    referrer as (select * from {{ ref("greenhouse_referrers_source") }}),
    source as (select * from {{ ref("greenhouse_sources_source") }}),
    candidate as (select * from {{ ref("greenhouse_candidates_source") }}),
    intermediate as (

        select
            application_id,
            applications.candidate_id,
            applications.referrer_id,
            referrer_name as sourcer_name,
            applied_at as application_date,
            candidate_created_at
        from applications
        left join referrer on applications.referrer_id = referrer.referrer_id
        left join source on applications.source_id = source.source_id
        left join candidate on applications.candidate_id = candidate.candidate_id
        where source.source_type = 'Prospecting'

    )

select *
from intermediate
