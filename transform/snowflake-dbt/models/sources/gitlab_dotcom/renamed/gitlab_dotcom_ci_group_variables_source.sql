with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_group_variables_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_group_variable_id,
            key as key,
            group_id::number as ci_group_variable_group_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            masked as masked,
            variable_type as variable_variable_type
        from source

    )


select *
from renamed
