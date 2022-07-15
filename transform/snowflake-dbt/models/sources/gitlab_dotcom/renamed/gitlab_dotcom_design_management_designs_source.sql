with
    source as (

        select * from {{ ref("gitlab_dotcom_design_management_designs_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as design_id,
            project_id::number as project_id,
            issue_id::number as issue_id,
            filename::varchar as design_filename
        from source

    )

select *
from renamed
