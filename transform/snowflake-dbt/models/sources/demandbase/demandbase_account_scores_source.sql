with
    source as (select * from {{ source("demandbase", "account_scores") }}),
    renamed as (

        select
            jsontext['account_domain']::varchar as account_domain,
            jsontext['account_id']::number as account_id,
            jsontext['score']::varchar as account_score,
            jsontext['score_type']::varchar as score_type,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
