with
    source as (select * from {{ ref("gitlab_dotcom_keys_dedupe_source") }}),
    renamed as (

        select

            id::number as key_id,
            user_id::number as user_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            type::varchar as key_type,
            public::boolean as is_public,
            last_used_at::timestamp as last_updated_at

        from source

    )

select *
from renamed
