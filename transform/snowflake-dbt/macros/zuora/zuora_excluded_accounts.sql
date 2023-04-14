{% macro zuora_excluded_accounts() %}

    select distinct account_id
    from {{ ref("zuora_excluded_accounts") }}
    where account_id is not null

{% endmacro %}
