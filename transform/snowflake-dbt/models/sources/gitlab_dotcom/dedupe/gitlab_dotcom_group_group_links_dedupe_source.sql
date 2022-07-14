{{ config({"materialized": "incremental", "unique_key": "id"}) }}

select *
from {{ source("gitlab_dotcom", "group_group_links") }}
{% if is_incremental() %}

where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() OVER (partition by id order by updated_at desc) = 1
