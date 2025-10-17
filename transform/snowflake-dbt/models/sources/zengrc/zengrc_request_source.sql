with
    source as (select * from {{ source("zengrc", "requests") }}),

    renamed as (

        select
            assignees::variant as assignees,
            audit__id::number as audit_id,
            audit__title::varchar as audit_title,
            code::varchar as request_code,
            created_at::timestamp as request_created_at,
            custom_attributes::variant as request_custom_attributes,
            description::varchar as request_description,
            end_date::date as request_end_date,
            id::number as request_id,
            mapped__controls::variant as mapped_controls,
            mapped__issues::variant as mapped_issues,
            mapped__programs::variant as mapped_programs,
            requesters::variant as requestors,
            start_date::date as request_start_date,
            status::varchar as request_status,
            stop_date::date as request_stop_date,
            tags::varchar as request_tags,
            title::varchar as request_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as request_updated_at,
            __loaded_at::timestamp as request_loaded_at,
            parse_json(custom_attributes)['209']['value']::varchar as arr_impact,
            parse_json(custom_attributes)['68']['value']::varchar as audit_period,
            parse_json(custom_attributes)['173']['value']::varchar as caa_activity_type,
            parse_json(custom_attributes)['70']['value']::boolean as is_external_audit,
            parse_json(custom_attributes)['59']['value']::varchar as gitlab_assignee,
            parse_json(custom_attributes)['60']['value']::varchar as gitlab_issue_url,
            parse_json(custom_attributes)['69']['value']::varchar as priority_level

        from source

    )

select *
from renamed
