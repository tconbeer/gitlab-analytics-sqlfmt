with
    source as (select * from {{ source("netsuite", "budget_category") }}),
    renamed as (

        select
            -- Primary Key
            budget_category_id::float as budget_category_id,

            -- Info
            isinactive::boolean as is_inactive,
            is_global::boolean as is_global,
            name::varchar as budget_category,
            _fivetran_deleted::boolean as is_fivetran_deleted

        from source

    )

select *
from renamed
