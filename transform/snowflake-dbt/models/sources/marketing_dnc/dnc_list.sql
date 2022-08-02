with
    source as (select * from {{ source("marketing_dnc", "dnc_list") }}),
    renamed as (

        select
            email_address::varchar as email_address,
            is_role::boolean as is_role,
            is_disposable::boolean as is_disposable,
            did_you_mean::varchar as did_you_mean,
            result::varchar as result,
            reason::varchar as reason,
            risk::varchar as risk,
            root_email_address::varchar as root_email_address
        from source

    )

select *
from renamed
