with
    source as (select * from {{ source("zengrc", "controls") }}),

    renamed as (

        select
            code::varchar as control_code,
            created_at::timestamp as control_created_at,
            description::varchar as control_description,
            id::number as control_id,
            mapped__objectives::variant as mapped_objectives,
            status::varchar as control_status,
            title::varchar as control_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as control_updated_at,
            __loaded_at::timestamp as control_loaded_at,
            parse_json(custom_attributes) [
                '187'
            ] ['value']::varchar as application_used,
            parse_json(custom_attributes) [
                '106'
            ] ['value']::varchar as average_control_effectiveness_rating,
            parse_json(custom_attributes) [
                '107'
            ] ['value']::varchar as control_deployment,
            parse_json(custom_attributes) ['116'] ['value']::varchar as control_level,
            parse_json(custom_attributes) [
                '182'
            ] ['value']::varchar as control_objective,
            parse_json(custom_attributes) [
                '181'
            ] ['value']::varchar as control_objective_ref_number,
            parse_json(custom_attributes) ['186'] ['value']::varchar as control_type,
            parse_json(custom_attributes) ['214'] ['value']::varchar as coso_components,
            parse_json(custom_attributes) ['215'] ['value']::varchar as coso_principles,
            parse_json(custom_attributes) ['174'] ['value']::varchar as financial_cycle,
            parse_json(custom_attributes) [
                '213'
            ] ['value']::varchar as financial_statement_assertions,
            parse_json(custom_attributes) ['117'] ['value']::varchar as gitlab_guidance,
            parse_json(custom_attributes) ['205'] ['value']::varchar as ia_significance,
            parse_json(custom_attributes) ['196'] ['value']::varchar as ia_test_plan,
            parse_json(custom_attributes) ['192'] ['value']::varchar as ipe_report_name,
            parse_json(custom_attributes) [
                '193'
            ] ['value']::varchar as ipe_review_parameters,
            parse_json(custom_attributes) [
                '188'
            ] ['value']::varchar as management_review_control,
            parse_json(custom_attributes) [
                '183'
            ] ['value']::varchar as mitigating_control_activities,
            parse_json(custom_attributes) [
                '112'
            ] ['value']::varchar as original_scf_control_language,
            parse_json(custom_attributes) ['175'] ['value']::varchar as process,
            parse_json(custom_attributes) [
                '184'
            ] ['value']::varchar as process_owner_name,
            parse_json(custom_attributes) [
                '185'
            ] ['value']::varchar as process_owner_title,
            parse_json(custom_attributes) [
                '12'
            ] ['value']::number as relative_control_weighting,
            parse_json(custom_attributes) ['179'] ['value']::varchar as risk_event,
            parse_json(custom_attributes) ['178'] ['value']::varchar as risk_ref_number,
            parse_json(custom_attributes) [
                '10'
            ] ['value']::varchar as sample_control_implementation,
            parse_json(custom_attributes) [
                '11'
            ] ['value']::varchar as scf_control_question,
            parse_json(custom_attributes) [
                '194'
            ] ['value']::varchar as spreadsheet_name,
            parse_json(custom_attributes) [
                '195'
            ] ['value']::varchar as spreadsheet_review_parameters,
            parse_json(custom_attributes) ['176'] ['value']::varchar as sub_process


        from source

    )

select *
from renamed
