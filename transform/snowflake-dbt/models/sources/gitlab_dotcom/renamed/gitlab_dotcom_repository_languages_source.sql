with
    source as (

        select * from {{ ref("gitlab_dotcom_repository_languages_dedupe_source") }}

    ),
    renamed as (

        select
            md5(project_programming_language_id)::varchar as repository_language_id,
            project_id::number as project_id,
            programming_language_id::number as programming_language_id,
            share::float as share
        from source

    )

select *
from renamed
