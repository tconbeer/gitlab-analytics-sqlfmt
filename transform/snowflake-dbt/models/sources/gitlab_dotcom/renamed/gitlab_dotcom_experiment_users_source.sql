with
    source as (select * from {{ ref("gitlab_dotcom_experiment_users_dedupe_source") }}),
    renamed as (

        select
            context::variant as context,
            converted_at::timestamp as converted_at,
            created_at::timestamp as created_at,
            experiment_id::number as experiment_id,
            id::number as experiment_user_id,
            group_type::number as group_type,
            updated_at::timestamp as updated_at,
            user_id::number as user_id
        from source

    )

select *
from renamed
