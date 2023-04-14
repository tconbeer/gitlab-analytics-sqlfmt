with
    demandbase_date as (

        {% set tables = [
            "account",
            "account_keyword_historical_rollup",
            "account_keyword_intent",
            "account_list",
            "account_list_account",
            "account_scores",
            "account_site_page_metrics",
            "campaign_account_performance",
            "keyword_set",
            "keyword_set_keyword",
        ] %}

        {% for table in tables %}
            select '{{table}}' as table_name, max(uploaded_at) as max_date
            from {{ source("demandbase", table) }}

            {% if not loop.last %}
                union all
            {% endif %}

        {% endfor %}

    )

select *
from demandbase_date
