with
    source as (

        select *
        from {{ ref("gitlab_dotcom_ci_build_trace_section_names_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_build_id,
            project_id::number as project_id,
            name::varchar as ci_build_name

        from source

    )


select *
from renamed
