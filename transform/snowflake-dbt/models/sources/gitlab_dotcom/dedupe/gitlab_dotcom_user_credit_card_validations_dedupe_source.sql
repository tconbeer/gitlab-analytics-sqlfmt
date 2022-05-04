{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'user_credit_card_validations') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _uploaded_at DESC) = 1