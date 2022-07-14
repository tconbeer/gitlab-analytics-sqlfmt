with
    source as (select * from {{ source("zengrc", "objectives") }}),

    renamed as (

        select
            code::varchar as objective_code,
            created_at::timestamp as objective_created_at,
            description::varchar as objective_description,
            id::number as objective_id,
            os_state::varchar as objective_os_state,
            status::varchar as objective_status,
            title::varchar as objective_title,
            type::varchar as zengrc_object_type,
            updated_at::timestamp as objective_updated_at,
            __loaded_at::timestamp as objective_loaded_at,
            parse_json(custom_attributes) ['3'] ['value']::varchar as fedramp_parameter,
            parse_json(custom_attributes) ['219'] ['value']::varchar
            as security_requirement_nist_800_171

        from source

    )

select *
from renamed
