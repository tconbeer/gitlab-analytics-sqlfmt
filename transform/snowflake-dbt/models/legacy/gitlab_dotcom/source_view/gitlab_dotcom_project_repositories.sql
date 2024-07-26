{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_project_repositories_source") }})

select *
from source
