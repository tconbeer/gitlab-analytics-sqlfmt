{{ config({"materialized": "incremental", "unique_key": "namespace_id"}) }}

select *
from {{ source("gitlab_dotcom", "elasticsearch_indexed_namespaces") }}
{% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by namespace_id order by updated_at desc) = 1
