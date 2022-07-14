with
    source as (

        select *
        from {{ source("customers", "customers_db_trial_histories") }}
        qualify
            row_number() over (partition by gl_namespace_id order by updated_at desc)
            = 1

    ),
    renamed as (

        select distinct
            gl_namespace_id::varchar as gl_namespace_id,
            start_date::timestamp as start_date,
            expired_on::timestamp as expired_on,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            glm_source::varchar as glm_source,
            glm_content::varchar as glm_content,
            trial_entity::varchar as trial_entity
        from source

    )

select *
from renamed
