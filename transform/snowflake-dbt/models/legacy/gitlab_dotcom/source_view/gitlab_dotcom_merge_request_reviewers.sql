{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_merge_request_reviewers_source") }})

select *
from source
