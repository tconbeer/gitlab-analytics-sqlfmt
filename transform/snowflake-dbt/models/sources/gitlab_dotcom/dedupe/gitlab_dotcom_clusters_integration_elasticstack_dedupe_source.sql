{{ config({"materialized": "incremental", "unique_key": "cluster_id"}) }}

select *
from {{ source("gitlab_dotcom", "clusters_integration_elasticstack") }}
{% if is_incremental() %}

where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by cluster_id order by _uploaded_at desc) = 1
