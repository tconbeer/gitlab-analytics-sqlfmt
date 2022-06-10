with
    source as (select * from {{ source("sheetload", "deleted_mrs") }}),
    renamed as (

        select deleted_merge_request_id::integer as deleted_merge_request_id from source

    )

select *
from renamed
