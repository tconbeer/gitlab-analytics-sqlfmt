{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "project_programming_language_id",
        }
    )
}}

select *
from {{ source("gitlab_dotcom", "repository_languages") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify
    row_number() over (
        partition by project_programming_language_id order by _uploaded_at desc
    )
    = 1
