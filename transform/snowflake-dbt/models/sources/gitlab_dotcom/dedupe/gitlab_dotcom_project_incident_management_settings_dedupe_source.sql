
{{ config({"materialized": "incremental", "unique_key": "project_id"}) }}


select *
from {{ source("gitlab_dotcom", "project_incident_management_settings") }}
{% if is_incremental() %}

where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() OVER (partition by project_id order by _uploaded_at desc) = 1
