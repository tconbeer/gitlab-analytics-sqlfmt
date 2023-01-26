{% set year_value = var("year", run_started_at.strftime("%Y")) %}
{% set month_value = var("month", run_started_at.strftime("%m")) %}

with
    base as (

        select distinct event_id, contexts
        {% if target.name not in ("prod") -%}

        from {{ ref("snowplow_gitlab_good_events_sample_source") }}

        {%- else %} from {{ ref("snowplow_gitlab_good_events_source") }}

        {%- endif %}

        where
            app_id is not null
            and date_part(month, try_to_timestamp(derived_tstamp)) = '{{ month_value }}'
            and date_part(year, try_to_timestamp(derived_tstamp)) = '{{ year_value }}'
            and (
                (v_tracker like 'js%')  -- js frontend tracker
                or (v_tracker like 'rb%')  -- ruby backend tracker
            )
            and try_to_timestamp(derived_tstamp) is not null

    ),

    events_with_context_flattened as (

        select
            base.*,
            flat_contexts.value['schema']::varchar as context_data_schema,
            try_parse_json(flat_contexts.value['data']) as context_data
        from base
        inner join
            lateral flatten(
                input => try_parse_json(contexts), path => 'data'
            ) as flat_contexts

    ),

    experiment_contexts as (

        select distinct  -- Some event_id are not unique dispite haveing the same experiment context as discussed in MR 6288
            event_id,
            context_data['experiment']::varchar as experiment_name,
            context_data['key']::varchar as context_key,
            context_data['variant']::varchar as experiment_variant,
            array_to_string(
                context_data['migration_keys']::variant, ', '
            ) as experiment_migration_keys
        from events_with_context_flattened
        where
            lower(context_data_schema)
            like 'iglu:com.gitlab/gitlab_experiment/jsonschema/%'

    )

select *
from experiment_contexts
