
{{ config({"materialized": "incremental", "unique_key": "issue_id"}) }}


select *
from {{ source("gitlab_dotcom", "issues_prometheus_alert_events") }}
{% if is_incremental() %}

where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() OVER (partition by issue_id order by updated_at desc) = 1
