{{ config({"schema": "legacy"}) }}

with
    source as (

        select *
        from {{ ref("gitlab_dotcom_namespace_root_storage_statistics_source") }}

    )

select *
from source
