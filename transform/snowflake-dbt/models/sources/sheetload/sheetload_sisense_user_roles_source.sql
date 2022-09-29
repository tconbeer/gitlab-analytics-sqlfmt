with
    source as (select * from {{ source("sheetload", "sisense_user_roles") }}),
    renamed as (

        select
            id::varchar as id,
            updated_at::timestamp as updated_at,
            role_id::varchar as role_id,
            user_id::varchar as user_id,
            space_id::varchar as space_id

        from source
    )

select *
from renamed
