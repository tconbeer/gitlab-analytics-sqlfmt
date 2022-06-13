with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_crossplane_source") }}

    )

select *
from source
