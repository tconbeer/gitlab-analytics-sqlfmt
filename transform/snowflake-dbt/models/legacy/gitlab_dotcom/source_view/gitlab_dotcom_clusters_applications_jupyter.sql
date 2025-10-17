with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_jupyter_source") }}

    )

select *
from source
