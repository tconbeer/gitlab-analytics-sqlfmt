with
    source as (

        select *
        from {{ ref("gitlab_dotcom_elasticsearch_indexed_namespaces_dedupe_source") }}

    ),
    types_cast as (

        select
            namespace_id::number as namespace_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            _uploaded_at::number::timestamp as uploaded_at,
            _task_instance::varchar as task_instance_name
        from source

    )

select *
from types_cast
