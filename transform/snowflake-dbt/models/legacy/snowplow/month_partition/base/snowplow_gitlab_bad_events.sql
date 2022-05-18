{% set year_value = var("year", run_started_at.strftime("%Y")) %}
{% set month_value = var("month", run_started_at.strftime("%m")) %}

{{ config({"unique_key": "bad_event_surrogate"}) }}

with
    base as (

        select *
        from {{ ref("snowplow_gitlab_bad_events_source") }}
        where
            length(jsontext['errors']) > 0 and date_part(
                month, jsontext['failure_tstamp']::timestamp
            ) = '{{ month_value }}' and date_part(
                year, jsontext['failure_tstamp']::timestamp
            ) = '{{ year_value }}'

    ),
    renamed as (

        select 
      distinct
            jsontext['line']::varchar as base64_event,
            to_array(jsontext['errors']) as error_array,
            jsontext['failure_tstamp']::timestamp as failure_timestamp,
            'GitLab' as infra_source,
            uploaded_at,
            {{
                dbt_utils.surrogate_key(
                    ["base64_event", "failure_timestamp", "error_array"]
                )
            }} as bad_event_surrogate
        from base

    )

select *
from renamed
order by failure_timestamp
