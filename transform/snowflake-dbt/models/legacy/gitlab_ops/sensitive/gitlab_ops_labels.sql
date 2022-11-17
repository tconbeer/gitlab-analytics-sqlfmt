with source as (select * from {{ ref("gitlab_ops_labels_source") }})

select *
from source
