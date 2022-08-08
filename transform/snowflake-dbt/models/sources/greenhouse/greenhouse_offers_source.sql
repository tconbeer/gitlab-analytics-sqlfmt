with
    source as (select * from {{ source("greenhouse", "offers") }}),
    renamed as (

        select
            id as offer_id,

            -- keys
            application_id,

            -- info
            status as offer_status,
            created_by,
            start_date::date as start_date,

            created_at::timestamp as created_at,
            sent_at::timestamp as sent_at,
            resolved_at::timestamp as resolved_at,
            updated_at::timestamp as updated_at

        from source
        where offer_status != 'deprecated'

    )

select *
from renamed
