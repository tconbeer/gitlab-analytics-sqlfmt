with
    source as (select * from {{ source("zendesk_community_relations", "tags") }}),

    renamed as (select count as tag_count, name as tag_name from source)

select *
from renamed
