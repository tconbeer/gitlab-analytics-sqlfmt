with
    source as (select * from {{ source("demandbase", "account_keyword_intent") }}),
    renamed as (

        select
            jsontext['account_id']::number as account_id,
            jsontext['intent_strength']::varchar as intent_strength,
            jsontext['is_trending']::boolean as is_trending,
            jsontext['keyword']::varchar as keyword,
            jsontext['keyword_set_id']::number as keyword_set_id,
            jsontext['people_researching_count']::number as people_researching_count,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
