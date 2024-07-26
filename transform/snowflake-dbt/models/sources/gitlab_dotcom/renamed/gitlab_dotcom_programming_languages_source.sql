with
    source as (

        select * from {{ ref("gitlab_dotcom_programming_languages_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as programming_language_id,
            name::varchar as programming_language_name
        from source

    )

select *
from renamed
