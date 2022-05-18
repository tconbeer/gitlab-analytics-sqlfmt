with
    source as (select * from {{ source("zengrc", "audits") }}),

    renamed as (

        select
            audit_managers::variant as audit_managers,
            code::varchar as audit_code,
            created_at::timestamp as audit_created_at,
            description::varchar as audidt_description,
            end_date::date as audit_end_date,
            id::number as audit_id,
            mapped__markets::variant as mapped_markets,
            mapped__standards::variant as mapped_standards,
            program__id::number as program_id,
            program__title::varchar as program_title,
            program__type::varchar as program_type,
            report_period_end_date::date as audit_report_period_end_date,
            report_period_start_date::date as audit_report_period_start_date,
            start_date::date as audit_start_date,
            status::varchar as audit_status,
            sync_external_attachments::boolean as has_external_attachments,
            sync_external_comments::boolean as has_external_comments,
            title::varchar as audit_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as audit_uploaded_at,
            __loaded_at::timestamp as audit_loaded_at,
            parse_json(custom_attributes) ['66'] ['value']::varchar as audit_category,
            parse_json(custom_attributes) [
                '212'
            ] ['value']::date as audit_completion_date,
            parse_json(custom_attributes) [
                '206'
            ] ['value']::varchar as delegated_testing_owner,
            parse_json(custom_attributes) [
                '216'
            ] ['value']::date as documentation_due_date,
            parse_json(custom_attributes) ['217'] ['value']::date as escalation_date,
            parse_json(custom_attributes) ['123'] ['value']::varchar as gitlab_assignee,
            parse_json(custom_attributes) ['149'] ['value']::varchar as inherent_risk,
            parse_json(custom_attributes) [
                '152'
            ] ['value']::varchar as period_completed,
            parse_json(custom_attributes) ['151'] ['value']::varchar as period_created,
            parse_json(custom_attributes) ['150'] ['value']::varchar as residual_risk,
            parse_json(custom_attributes) [
                '147'
            ] ['value']::varchar as system_effectiveness_rating,
            parse_json(custom_attributes) [
                '121'
            ] ['value']::varchar as system_tier_level

        from source

    )

select *
from renamed
