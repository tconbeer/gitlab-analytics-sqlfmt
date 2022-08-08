with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_prometheus_source") }}

    )

select *
from source
