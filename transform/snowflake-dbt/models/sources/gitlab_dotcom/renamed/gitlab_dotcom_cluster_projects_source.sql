with
    source as (select * from {{ ref("gitlab_dotcom_cluster_projects_dedupe_source") }})

    ,
    renamed as (

        select
            id::number as cluster_project_id,
            cluster_id::number as cluster_id,
            project_id::number as project_id

        from source

    )

select *
from renamed
