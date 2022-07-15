with
    source as (

        select * from {{ source("sheetload", "manual_downgrade_dotcom_tracking") }}

    ),
    renamed as (

        select
            namespace_id::number as namespace_id,
            try_to_date(downgraded_date) as downgraded_date
        from source

    )

select *
from renamed
