with
    source as (

        select * from {{ ref("gitlab_dotcom_elasticsearch_indexed_namespaces") }}

    ),
    latest as (

        select namespace_id, created_at, updated_at
        from source
        where task_instance_name = (select max(task_instance_name) from source)

    )

select *
from latest
