with
    snowplow_date as (

        {% set tables = ["events", "events_sample", "bad_events"] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(uploaded_at) as max_date
        from {{ source("gitlab_snowplow", table) }}


        {% if not loop.last %} union all {% endif %}

        {% endfor %}

    )


select *
from snowplow_date
