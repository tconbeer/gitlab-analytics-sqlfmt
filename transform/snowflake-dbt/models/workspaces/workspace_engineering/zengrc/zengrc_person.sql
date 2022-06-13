{{
    simple_cte(
        [
            ("audits", "zengrc_audit_source"),
            ("assessments", "zengrc_assessment_source"),
            ("requests", "zengrc_request_source"),
        ]
    )
}}

,
audit_managers as (

    select distinct
        audut_manager.value['id']::number as person_id,
        audut_manager.value['name']::varchar as person_name,
        audut_manager.value['type']::varchar as zengrc_type
    from audits
    inner join
        lateral flatten(input => try_parse_json(audits.audit_managers)) audut_manager

),
assessors as (

    select distinct
        assessors.value['id']::number as person_id,
        assessors.value['name']::varchar as person_name,
        assessors.value['type']::varchar as zengrc_type
    from assessments
    inner join lateral flatten(input => try_parse_json(assessments.assessors)) assessors

),
assignees as (

    select distinct
        assignees.value['id']::number as person_id,
        assignees.value['name']::varchar as person_name,
        assignees.value['type']::varchar as zengrc_type
    from requests
    inner join lateral flatten(input => try_parse_json(requests.assignees)) assignees

),
requestors as (

    select distinct
        requestors.value['id']::number as person_id,
        requestors.value['name']::varchar as person_name,
        requestors.value['type']::varchar as zengrc_type
    from requests
    inner join lateral flatten(input => try_parse_json(requests.requestors)) requestors

),
unioned as (

    select *
    from audit_managers

    union

    select *
    from assessors

    union

    select *
    from assignees

    union

    select *
    from requestors
)

select *
from unioned
