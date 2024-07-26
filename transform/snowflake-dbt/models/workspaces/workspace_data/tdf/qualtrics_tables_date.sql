with
    qualtrics_date as (

        {% set tables = [
            "contact",
            "distribution",
            "nps_survey_responses",
            "post_purchase_survey_responses",
            "survey",
            "questions",
        ] %}

        {% for table in tables %}
            select '{{table}}' as table_name, max(uploaded_at) as max_date
            from {{ source("qualtrics", table) }}

            {% if not loop.last %}
                union all
            {% endif %}

        {% endfor %}

    )

select *
from qualtrics_date
