with source as (select * from {{ ref("gitlab_dotcom_clusters_source") }})

select *
from source
