with
    source as (select * from {{ source("bamboohr", "directory") }}),
    intermediate as (

        select
            value['id']::number as employee_id,
            value['displayName']::varchar as full_name,
            value['jobTitle']::varchar as job_title,
            value['supervisor']::varchar as supervisor,
            value['workEmail']::varchar as work_email,
            uploaded_at as uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true)

    ),
    final as (

        select *
        from intermediate
        where
            work_email != 't2test@gitlab.com' and (
                lower(full_name) not like '%greenhouse test%' and lower(
                    full_name
                ) not like '%test profile%' and lower(full_name) != 'test-gitlab'
            ) and employee_id not in (42039, 42043) and uploaded_at not in (
                '2021-03-24 22:00:47.283',
                '2021-03-24 20:01:27.458',
                '2021-03-24 18:01:33.370'
            )

    )

select *
from final
