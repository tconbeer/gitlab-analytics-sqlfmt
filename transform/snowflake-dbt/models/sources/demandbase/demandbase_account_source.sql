with
    source as (select * from {{ source("demandbase", "account") }}),
    renamed as (

        select distinct
            jsontext['account_domain']::varchar as account_domain,
            jsontext['account_id']::number as account_id,
            jsontext['account_name']::varchar as account_name,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
