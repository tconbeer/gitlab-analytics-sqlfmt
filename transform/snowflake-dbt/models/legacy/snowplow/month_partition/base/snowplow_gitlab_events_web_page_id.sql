{% set year_value = var("year", run_started_at.strftime("%Y")) %}
{% set month_value = var("month", run_started_at.strftime("%m")) %}

{{ config({"unique_key": "event_id"}) }}

{% set change_form = [
    "formId",
    "elementId",
    "nodeName",
    "type",
    "elementClasses",
    "value",
] %}
{% set submit_form = ["formId", "formClasses", "elements"] %}
{% set focus_form = [
    "formId",
    "elementId",
    "nodeName",
    "elementType",
    "elementClasses",
    "value",
] %}
{% set link_click = [
    "elementId",
    "elementClasses",
    "elementTarget",
    "targetUrl",
    "elementContent",
] %}
{% set track_timing = ["category", "variable", "timing", "label"] %}


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
    we need to extract the web_page_id from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The context we are looking for containing the web_page_id is this one:
      {
      'data': {
      'id': 'de5069f7-32cf-4ad4-98e4-dafe05667089'
      },
      'schema': 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
      }
    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data (where the web_page_id will be contained)
    */
        select
            base.*,
            f.value['schema']::text as context_data_schema,
            f.value['data'] as context_data
        from base, lateral flatten(input => try_parse_json(contexts), path => 'data') f

    )

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the web_page context, which has this context schema: iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0
Then we extract the id from the context_data column
*/
select events_with_context_flattened.event_id, context_data['id']::text as web_page_id
from events_with_context_flattened
where
    context_data_schema
    = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
