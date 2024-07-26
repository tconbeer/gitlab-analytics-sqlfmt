with
    source as (select * from {{ source("zuora_query_api", "users") }}),
    renamed as (

        select
            "Id"::text as zuora_user_id,
            "Email"::text as email,
            "FirstName"::text as first_name,
            "LastName"::text as last_name,
            "Username"::text as user_name,
            to_timestamp(convert_timezone('UTC', "CreatedDate"))::timestamp
            as created_date,
            to_timestamp_ntz(cast(_uploaded_at as int))::timestamp as uploaded_at
        from source

    )

select *
from renamed
