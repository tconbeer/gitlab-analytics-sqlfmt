with
    source as (select * from {{ source("demandbase", "account_list") }}),
    renamed as (

        select
            jsontext['creation_time']::timestamp as created_at,
            jsontext['id']::number as account_list_id,
            jsontext['name']::varchar as account_list_name,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
