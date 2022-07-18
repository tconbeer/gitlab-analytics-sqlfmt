{{ config({"materialized": "incremental", "unique_key": "user_id"}) }}

select *
from {{ source("gitlab_dotcom", "user_preferences") }}
{% if is_incremental() %}

where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by user_id order by updated_at desc) = 1
