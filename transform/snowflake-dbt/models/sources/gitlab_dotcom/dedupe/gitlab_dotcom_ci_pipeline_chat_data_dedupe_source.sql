{{ config({"materialized": "incremental", "unique_key": "pipeline_id"}) }}

select *
from {{ source("gitlab_dotcom", "ci_pipeline_chat_data") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by pipeline_id order by _uploaded_at desc) = 1
