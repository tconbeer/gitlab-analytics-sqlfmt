{{ config({"materialized": "incremental", "unique_key": "id"}) }}

select *
from {{ source("gitlab_dotcom", "resource_weight_events") }}
{% if is_incremental() %}

where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() OVER (partition by id order by _uploaded_at desc) = 1
