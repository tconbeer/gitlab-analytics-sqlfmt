with
    source as (

        select *
        from {{ ref("gitlab_dotcom_clusters_applications_elastic_stacks_source") }}

    )

select *
from source
