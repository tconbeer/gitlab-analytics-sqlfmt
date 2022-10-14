{{ config({"materialized": "incremental", "unique_key": "project_id"}) }}


select *
from {{ source("gitlab_dotcom", "container_expiration_policies") }}
{% if is_incremental() %}

where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by project_id order by updated_at desc) = 1
