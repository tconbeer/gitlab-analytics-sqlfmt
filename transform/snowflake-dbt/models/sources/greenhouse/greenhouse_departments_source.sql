with
    source as (select * from {{ source("greenhouse", "departments") }}),
    renamed as (

        select

            -- keys
            id::number as department_id,
            organization_id::number as organization_id,
            parent_id::number as parent_id,

            -- info
            name::varchar(100) as department_name,
            created_at::timestamp as department_created_at,
            updated_at::timestamp as department_updated_at

        from source

    )

select
    department_id,
    organization_id,
    parent_id,
    replace(replace(department_name, ')', ''), '(', '')::varchar(
        100
    ) as department_name,
    department_created_at,
    department_updated_at
from renamed
