
{{ config({
    "materialized": "incremental",
    "unique_key": "issue_id"
    })
}}


SELECT *
FROM {{ source('gitlab_dotcom', 'issues_self_managed_prometheus_alert_events') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY updated_at DESC) = 1
