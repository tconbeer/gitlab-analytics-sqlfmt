{{ config({"materialized": "incremental", "unique_key": "id"}) }}


select *
from {{ source("gitlab_dotcom", "audit_events_external_audit_event_destinations") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by id order by _uploaded_at desc) = 1
