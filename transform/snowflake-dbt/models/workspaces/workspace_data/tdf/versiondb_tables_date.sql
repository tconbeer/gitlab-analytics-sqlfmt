with
    versiondb_date as (

        {% set tables = [
    "conversational_development_indices",
    "fortune_companies",
    "hosts",
    "usage_data",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(updated_at) as max_date
        from {{ source("version", table) }}


        {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

        UNION ALL

        {% set tables = ["raw_usage_data", "versions", "version_checks"] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(created_at) as max_date
        from {{ source("version", table) }}


        {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

    )

select *
from versiondb_date
