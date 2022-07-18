with
    source as (select * from {{ source("zengrc", "risks") }}),

    renamed as (

        select
            code::varchar as risk_code,
            created_at::timestamp as risk_created_at,
            custom_attributes::variant as risk_custom_attributes,
            description::varchar as risk_description,
            id::number as risk_id,
            risk_vector_score_values::variant as risk_vector_score_values,
            status::varchar as risk_status,
            title::varchar as risk_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as risk_updated_at,
            __loaded_at::timestamp as risk_loaded_at,
            parse_json(custom_attributes)[
                '156'
            ]['value']::varchar as acceptance_of_risk_ownership,
            parse_json(custom_attributes)['44']['value']::varchar as cia_impact,
            parse_json(custom_attributes)['79']['value']::date as risk_identified_date,
            parse_json(custom_attributes)[
                '81'
            ]['value']::varchar as existing_mitigations,
            parse_json(custom_attributes)['57']['value']::varchar as interested_parties,
            parse_json(custom_attributes)[
                '158'
            ]['value']::boolean as is_risk_ready_for_review_and_closure,
            parse_json(custom_attributes)['46']['value']::varchar as risk_owner,
            parse_json(custom_attributes)['74']['value']::varchar as risk_tier,
            parse_json(custom_attributes)[
                '160'
            ]['value']::date as risk_treatment_completion_date,
            parse_json(custom_attributes)[
                '159'
            ]['value']::varchar as risk_treatment_option_selected,
            parse_json(custom_attributes)['45']['value']::varchar as root_cause,
            parse_json(custom_attributes)['58']['value']::varchar as threat_source,
            parse_json(custom_attributes)['148']['value']::boolean as is_tprm_related,
            parse_json(custom_attributes)[
                '75'
            ]['value']::boolean as is_within_risk_appetite

        from source

    )

select *
from renamed
