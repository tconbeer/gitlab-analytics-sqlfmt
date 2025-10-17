with source as (select * from {{ ref("gitlab_dotcom_labels_source") }})

select *
from source
