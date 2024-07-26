{{ config({"materialized": "incremental", "unique_key": "user_id"}) }}

select *
from {{ source("gitlab_dotcom", "user_credit_card_validations") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by user_id order by _uploaded_at desc) = 1
