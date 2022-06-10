{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_ci_builds_source") }})

select *
from source
