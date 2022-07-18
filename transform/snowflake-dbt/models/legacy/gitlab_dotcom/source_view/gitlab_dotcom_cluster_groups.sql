with source as (select * from {{ ref("gitlab_dotcom_cluster_groups_source") }})

select *
from source
