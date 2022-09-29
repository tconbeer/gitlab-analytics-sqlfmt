with
    source as (select * from {{ source("demandbase", "keyword_set_keyword") }}),
    renamed as (

        select
            jsontext['keyword']::varchar as keyword,
            jsontext['keyword_set_id']::number as keyword_set_id,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
