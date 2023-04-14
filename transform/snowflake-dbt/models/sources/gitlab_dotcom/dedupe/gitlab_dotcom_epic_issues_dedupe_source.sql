{{ config({"materialized": "incremental", "unique_key": "issue_id"}) }}

select *
from {{ source("gitlab_dotcom", "epic_issues") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by issue_id order by _uploaded_at desc) = 1
