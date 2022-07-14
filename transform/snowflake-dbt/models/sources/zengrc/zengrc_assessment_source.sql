with
    source as (select * from {{ source("zengrc", "assessments") }}),

    renamed as (

        select
            assessors::variant as assessors,
            code::varchar as assessment_code,
            conclusion::varchar as assessment_conclusion,
            control__id::number as control_id,
            control__title::varchar as control_title,
            control__type::varchar as control_type,
            created_at::timestamp as assessment_created_at,
            description::varchar as assessment_description,
            end_date::date as assessment_end_date,
            id::number as assessment_id,
            mapped__audits::variant as mapped_audits,
            start_date::date as assessment_start_date,
            status::varchar as assessment_status,
            title::varchar as assessment_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as assessment_uploaded_at,
            __loaded_at::timestamp as assessment_loaded_at,
            parse_json(custom_attributes) ['141'] ['value']::varchar
            as annualized_population_size,
            parse_json(custom_attributes) ['169'] ['value']::varchar
            as competency_and_authority_of_control_owners,
            parse_json(custom_attributes) ['125'] ['value']::varchar
            as control_health_and_effectiveness_rating_cher,
            parse_json(custom_attributes) ['135'] ['value']::varchar
            as control_implementation_statement,
            parse_json(custom_attributes) ['172'] ['value']::varchar
            as criteria_for_investigation_and_follow_up,
            parse_json(custom_attributes) ['134'] ['value']::varchar
            as customer_hosting_option,
            parse_json(custom_attributes) ['165'] ['value']::varchar
            as gitlab_control_family,
            parse_json(custom_attributes) ['113'] ['value']::varchar
            as gitlab_control_owner,
            parse_json(custom_attributes) ['171'] ['value']::varchar
            as level_of_consistency_and_frequency,
            parse_json(custom_attributes) ['170'] ['value']::varchar
            as level_of_judgement_and_aggregation,
            parse_json(custom_attributes) ['39'] ['value']::varchar
            as population_date_range,
            parse_json(custom_attributes) ['140'] ['value']::varchar as population_size,
            parse_json(custom_attributes) ['167'] ['value']::varchar
            as purpose_and_appropriateness_of_the_control,
            parse_json(custom_attributes) ['168'] ['value']::varchar
            as risk_associated_with_the_control,
            parse_json(custom_attributes) ['139'] ['value']::varchar as sample_size,
            parse_json(custom_attributes) ['118'] ['value']::varchar as system_level,
            parse_json(custom_attributes) ['104'] ['value']::varchar
            as test_of_design_results,
            parse_json(custom_attributes) ['105'] ['value']::varchar
            as test_of_operating_effectiveness_results,
            parse_json(custom_attributes) ['128'] ['value']::number
            as total_hours_estimate

        from source

    )

select *
from renamed
