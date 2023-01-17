with source as (select * from {{ ref("gitlab_dotcom_award_emoji_dedupe_source") }})

select
    id::number as award_emoji_id,
    name::varchar as award_emoji_name,
    user_id::number as user_id,
    awardable_id::number as awardable_id,
    awardable_type::varchar as awardable_type
from source
