with
    source as (

        select * from {{ ref("gitlab_dotcom_user_preferences_source_non_dedupe") }}

    )

select *
from source
