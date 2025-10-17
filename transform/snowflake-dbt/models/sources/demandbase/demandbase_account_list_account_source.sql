with
    source as (select * from {{ source("demandbase", "account_list_account") }}),
    renamed as (

        select
            jsontext['account_id']::number as account_id,
            jsontext['account_list_id']::varchar as account_list_id,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
