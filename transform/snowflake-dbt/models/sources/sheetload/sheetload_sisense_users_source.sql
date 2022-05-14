with
    source as (select * from {{ source("sheetload", "sisense_users") }}),
    renamed as (

        select
            id::varchar as id,
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            email_address::varchar as email_address

        from source
    )

select *
from renamed
