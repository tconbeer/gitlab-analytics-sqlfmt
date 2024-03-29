{{ config({"materialized": "incremental", "unique_key": "build_id"}) }}

select *
from {{ source("gitlab_dotcom", "ci_builds_runner_session") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by build_id order by _uploaded_at desc) = 1
