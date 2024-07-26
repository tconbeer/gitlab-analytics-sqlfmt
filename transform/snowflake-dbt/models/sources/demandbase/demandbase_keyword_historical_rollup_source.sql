with
    source as (

        select * from {{ source("demandbase", "account_keyword_historical_rollup") }}

    ),
    renamed as (

        select
            jsontext['account_id']::number as account_id,
            jsontext['backward_ordinal']::number as backward_ordinal,
            jsontext['duration_count']::number as duration_count,
            jsontext['duration_type']::varchar as duration_type,
            jsontext['keyword']::varchar as keyword,
            jsontext['people_researching_count']::number as people_researching_count,
            jsontext['start_date']::date as start_date,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
