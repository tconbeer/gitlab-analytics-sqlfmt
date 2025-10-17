{{ config({"materialized": "incremental", "unique_key": "approval_project_rule_id"}) }}


select *
from {{ source("gitlab_dotcom", "approval_project_rules_protected_branches") }}
{% if is_incremental() %}

    where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

{% endif %}
qualify
    row_number() over (partition by approval_project_rule_id order by _uploaded_at desc)
    = 1
