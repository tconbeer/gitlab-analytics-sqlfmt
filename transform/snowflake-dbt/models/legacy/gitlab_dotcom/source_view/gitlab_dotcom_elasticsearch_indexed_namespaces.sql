with
    source as (

        select * from {{ ref("gitlab_dotcom_elasticsearch_indexed_namespaces_source") }}

    )

select *
from source
