{% set year_value = var("year", run_started_at.strftime("%Y")) %}
{% set month_value = var("month", run_started_at.strftime("%m")) %}

{{ config({"unique_key": "event_id"}) }}

with
    filtered_source as (

        select event_id, contexts
        {% if target.name not in ("prod") -%}

        from {{ ref("snowplow_gitlab_good_events_sample_source") }}

        {%- else %} from {{ ref("snowplow_gitlab_good_events_source") }}

        {%- endif %}

        where
            app_id is not null
            and date_part(month, try_to_timestamp(derived_tstamp)) = '{{ month_value }}'
            and date_part(year, try_to_timestamp(derived_tstamp)) = '{{ year_value }}'
            and (
                (
                    -- js backend tracker
                    v_tracker like 'js%'
                    and lower(page_url) not like 'https://staging.gitlab.com/%'
                    and lower(page_url) not like 'https://customers.stg.gitlab.com/%'
                    and lower(page_url) not like 'http://localhost:%'
                )

                or

                (
                    -- ruby backend tracker
                    v_tracker like 'rb%'
                )
            )
            and try_to_timestamp(derived_tstamp) is not null
    ),
    base as (select distinct * from filtered_source),
    events_with_context_flattened as (
        /*
    we need to extract the GitLab standard context fields from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The GitLab standard context which we are looking for is defined by schema at:
    https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/gitlab_standard/jsonschema/1-0-5

    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data
    */
        select
            base.*,
            f.value['schema']::varchar as context_data_schema,
            f.value['data'] as context_data
        from base, lateral flatten(input => try_parse_json(contexts), path => 'data') f

    )

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the gitlab standard context, which has this context schema: iglu:com.gitlab/gitlab_standard/jsonschema/1-0-5
Then we extract the id from the context_data column
*/
select
    events_with_context_flattened.event_id::varchar as event_id,
    context_data_schema,
    context_data['environment']::varchar as environment,
    try_parse_json(context_data['extra'])::variant as extra,
    context_data['namespace_id']::number as namespace_id,
    context_data['plan']::varchar as plan,
    context_data['google_analytics_id']::varchar as google_analytics_id,
    iff(
        google_analytics_id = '',
        null,
        split_part(google_analytics_id, '.', 3)
        || '.'
        || split_part(google_analytics_id, '.', 4)
    )::varchar as google_analytics_client_id,
    context_data['project_id']::number as project_id,
    context_data['user_id']::varchar as pseudonymized_user_id,
    context_data['source']::varchar as source
from events_with_context_flattened
where context_data_schema like 'iglu:com.gitlab/gitlab_standard/jsonschema/%'
