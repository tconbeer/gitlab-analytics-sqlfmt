with
    source as (select * from {{ ref("gitlab_dotcom_shards_dedupe_source") }}),
    renamed as (select id::number as shard_id, name::varchar as shard_name from source)


select *
from renamed
