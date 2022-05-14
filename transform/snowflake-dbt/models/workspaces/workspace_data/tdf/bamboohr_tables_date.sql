with
    bamboohr_date as (

        {% set tables = [
    "compensation",
    "custom_currency_conversion",
    "custom_currency_conversion",
    "directory",
    "emergency_contacts",
    "employment_status",
    "employment_status",
    "job_info",
    "id_employee_number_mapping",
    "meta_fields",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(uploaded_at) as max_date
        from {{ source("bamboohr", table) }}


        {% if not loop.last %} union all {% endif %}

        {% endfor %}

    )


select *
from bamboohr_date
