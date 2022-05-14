with
    source as (select * from {{ ref("gitlab_dotcom_cluster_groups_dedupe_source") }})

    ,
    renamed as (

        select

            id::number as cluster_group_id,
            cluster_id::number as cluster_id,
            group_id::number as group_id

        from source

    )

select *
from renamed
