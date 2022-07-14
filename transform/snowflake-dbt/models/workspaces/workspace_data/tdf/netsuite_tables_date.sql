with
    netsuite_date as (

        {% set tables = [
    "accounting_books",
    "accounting_periods",
    "accounts",
    "classes",
    "currencies",
    "customers",
    "departments",
    "entity",
    "subsidiaries",
    "transaction_lines",
    "vendors",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(date_last_modified) as max_date
        from {{ source("netsuite", table) }}


        {% if not loop.last %}
        union all
        {% endif %}

        {% endfor %}

    )


select *
from netsuite_date
