with
    source as (select * from {{ source("zendesk", "tags") }}),

    renamed as (select count as tag_count, name as tag_name from source)

select *
from renamed
