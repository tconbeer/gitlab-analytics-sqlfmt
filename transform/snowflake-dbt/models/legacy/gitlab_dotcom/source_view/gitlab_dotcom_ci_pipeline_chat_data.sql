with source as (select * from {{ ref("gitlab_dotcom_ci_pipeline_chat_data_source") }})

select *
from source
