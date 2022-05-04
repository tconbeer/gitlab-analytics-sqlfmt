{{ config({
    "materialized": "incremental",
    "unique_key": "build_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_builds_runner_session') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY build_id ORDER BY _uploaded_at DESC) = 1
