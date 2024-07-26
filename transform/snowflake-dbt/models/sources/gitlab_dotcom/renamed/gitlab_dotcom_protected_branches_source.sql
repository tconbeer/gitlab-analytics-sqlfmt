with
    source as (

        select * from {{ ref("gitlab_dotcom_protected_branches_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as protected_branch_id,
            name::varchar as protected_branch_name,
            project_id::varchar as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            code_owner_approval_required::boolean as is_code_owner_approval_required
        from source

    )

select *
from renamed
