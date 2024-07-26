with
    source as (select * from {{ source("greenhouse", "referrers") }}),
    renamed as (

        select
            id as referrer_id,
            name as referrer_name,
            -- keys
            organization_id,
            user_id,

            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
