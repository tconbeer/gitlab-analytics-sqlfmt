{{
    config(
        {"materialized": "incremental", "unique_key": "deployment_merge_request_id"}
    )
}}

select *
from {{ source("gitlab_dotcom", "deployment_merge_requests") }}
{% if is_incremental() %}

where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify
    row_number() OVER (
        partition by deployment_merge_request_id order by _uploaded_at desc
    )
    = 1
