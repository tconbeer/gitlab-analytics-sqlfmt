with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_ingress_source") }}

    )

select *
from source
