with
    source as (select * from {{ source("zengrc", "issues") }}),

    renamed as (

        select
            code::varchar as issue_code,
            created_at::timestamp as issue_created_at,
            custom_attributes::variant as issue_custom_attributes,
            description::varchar as issue_description,
            id::number as issue_id,
            mapped__audits::variant as mapped_audits,
            mapped__controls::variant as mapped_controls,
            mapped__programs::variant as mapped_programs,
            mapped__standards::variant as mapped_standards,
            notes::varchar as issue_notes,
            status::varchar as issue_status,
            stop_date::date as issue_stop_date,
            tags::varchar as issue_tags,
            title::varchar as issue_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as issue_updated_at,
            __loaded_at::timestamp as issue_loaded_at,
            parse_json(issue_custom_attributes)[
                '109'
            ]['value']::varchar as remediation_recommendations,
            parse_json(issue_custom_attributes)[
                '110'
            ]['value']::varchar as deficiency_range,
            parse_json(issue_custom_attributes)[
                '111'
            ]['value']::varchar as risk_rating,
            parse_json(issue_custom_attributes)[
                '115'
            ]['value']::varchar as observation_issue_owner,
            parse_json(issue_custom_attributes)[
                '142'
            ]['value']::number as likelihood,
            parse_json(issue_custom_attributes)[
                '143'
            ]['value']::number as impact,
            parse_json(issue_custom_attributes)[
                '153'
            ]['value']::varchar as gitlab_issue_url,
            parse_json(issue_custom_attributes)[
                '154'
            ]['value']::varchar as gitlab_assignee,
            parse_json(issue_custom_attributes)[
                '155'
            ]['value']::varchar as department,
            parse_json(issue_custom_attributes)[
                '197'
            ]['value']::varchar as type_of_deficiency,
            parse_json(issue_custom_attributes)[
                '199'
            ]['value']::varchar as internal_control_component,
            parse_json(issue_custom_attributes)[
                '200'
            ]['value']::varchar as severity_of_deficiency,
            parse_json(issue_custom_attributes)[
                '202'
            ]['value']::varchar as financial_system_line_item,
            parse_json(issue_custom_attributes)[
                '207'
            ]['value']::boolean as is_reported_to_audit_committee,
            parse_json(issue_custom_attributes)[
                '203'
            ]['value']::varchar as deficiency_theme,
            parse_json(issue_custom_attributes)[
                '204'
            ]['value']::varchar as remediated_evidence,
            parse_json(issue_custom_attributes)[
                '198'
            ]['value']::varchar as financial_assertion_affected_by_deficiency,
            parse_json(issue_custom_attributes)[
                '201'
            ]['value']::varchar as compensating_controls
        from source

    )

select *
from renamed
