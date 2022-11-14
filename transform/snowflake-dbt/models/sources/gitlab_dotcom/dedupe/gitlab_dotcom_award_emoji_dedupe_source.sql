with
    award_emoji as (

        select *
        from {{ source("gitlab_dotcom", "award_emoji") }}
        qualify row_number() over (partition by id order by _uploaded_at desc) = 1

    )

select *
from award_emoji
