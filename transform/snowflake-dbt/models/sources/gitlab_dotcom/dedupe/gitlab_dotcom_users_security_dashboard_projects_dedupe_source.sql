{{ config({"materialized": "incremental", "unique_key": "user_id"}) }}

select *
from {{ source("gitlab_dotcom", "users_security_dashboard_projects") }}
{% if is_incremental() %}

where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() OVER (partition by user_id order by _uploaded_at desc) = 1
