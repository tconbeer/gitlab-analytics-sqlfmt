with
    source as (select * from {{ ref("edcast_datasets") }}),
    renamed as (

        select
            id::varchar as id,
            name as name,
            number_of_columns::number as number_of_columns,
            created_at::timestamp as created_at,
            data_current_at::timestamp as data_current_at,
            pdp_enabled::boolean as pdp_enabled,
            number_of_rows::number as number_of_rows,
            owner_id::number as owner_id,
            owner_name::varchar as owner_name,
            updated_at::timestamp as updated_at,
            __loaded_at::timestamp as __loaded_at
        from source

    )

select *
from renamed
