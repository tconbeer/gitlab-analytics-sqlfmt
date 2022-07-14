with
    source as (

        select * from {{ ref("gitlab_dotcom_user_custom_attributes_dedupe_source") }}

    ),
    renamed as (

        select
            user_id::number as user_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            key::varchar as user_custom_key,
            value::varchar as user_custom_value
        from source
        qualify row_number() OVER (partition by id order by updated_at desc) = 1


    )

select *
from renamed
