{{ config({"schema": "legacy"}) }}

with
    source as (

        select * from {{ ref("gitlab_dotcom_project_repository_storage_moves_source") }}

    )

select *
from source
