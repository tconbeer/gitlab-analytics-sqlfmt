with
    source as (select * from {{ source("demandbase", "keyword_set") }}),
    renamed as (

        select
            jsontext['competitive']::boolean as is_competitive,
            jsontext['creation_time']::timestamp as created_at,
            jsontext['id']::number as keyword_set_id,
            jsontext['name']::varchar as name,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
