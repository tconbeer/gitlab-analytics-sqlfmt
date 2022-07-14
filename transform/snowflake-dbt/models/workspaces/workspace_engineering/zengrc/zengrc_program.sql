{{
    simple_cte(
        [
            ("audits", "zengrc_audit_source"),
            ("issues", "zengrc_issue_source"),
            ("requests", "zengrc_request_source"),
        ]
    )
}},
audit_programs as (

    select
        audits.program_id,
        audits.program_title,
        audits.program_type as zengrc_object_type
    from audits
    where audits.program_id is not null
    qualify
        row_number() OVER (
            partition by audits.program_id order by audit_uploaded_at desc
        )
        = 1

),
issue_programs as (

    select
        mapped_programs.value['id']::number as program_id,
        mapped_programs.value['title']::varchar as program_title,
        mapped_programs.value['type']::varchar as zengrc_type
    from issues
    inner join
        lateral flatten(input => try_parse_json(issues.mapped_programs)) mapped_programs
    qualify
        row_number() OVER (partition by program_id order by issue_updated_at desc) = 1

),
requests_programs as (

    select
        mapped_programs.value['id']::number as program_id,
        mapped_programs.value['title']::varchar as program_title,
        mapped_programs.value['type']::varchar as zengrc_type
    from requests
    inner join
        lateral flatten(
            input => try_parse_json(requests.mapped_programs)
        ) mapped_programs
    qualify
        row_number() OVER (partition by program_id order by request_updated_at desc) = 1

),
unioned as (

    select *
    from audit_programs

    union

    select *
    from issue_programs

    union

    select *
    from requests_programs

)

select *
from unioned
