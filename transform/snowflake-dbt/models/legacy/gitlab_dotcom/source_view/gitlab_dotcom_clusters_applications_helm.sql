with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_helm_source") }}

    )

select *
from source
