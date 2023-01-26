with
    source as (

        select * from {{ ref("gitlab_dotcom_clusters_applications_runners_source") }}

    )

select *
from source
