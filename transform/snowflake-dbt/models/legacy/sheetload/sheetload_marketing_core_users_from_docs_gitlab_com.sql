with
    source as (

        select *
        from {{ ref("sheetload_marketing_core_users_from_docs_gitlab_com_source") }}

    )

select *
from source
