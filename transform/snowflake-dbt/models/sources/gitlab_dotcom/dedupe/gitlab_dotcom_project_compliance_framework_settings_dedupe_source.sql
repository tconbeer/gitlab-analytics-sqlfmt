
{{ config({
    "materialized": "incremental",
    "unique_key": "project_id"
    })
}}


SELECT *
FROM {{ source('gitlab_dotcom', 'project_compliance_framework_settings') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY _uploaded_at DESC) = 1
