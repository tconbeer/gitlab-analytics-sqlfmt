with
    events as (select * from {{ ref("snowplow_structured_events") }}),
    contexts as (

        select context.value['data'] as performance_timing, event_id, derived_tstamp
        from events, lateral flatten(input => contexts, path => 'data') as context
        where context.value['schema'] = 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0'

    ),
    parsed_timing as (

        select
            performance_timing['connectEnd'] as connect_end,
            performance_timing['connectStart'] as connect_start,
            performance_timing['domComplete'] as dom_complete,
            performance_timing[
                'domContentLoadedEventEnd'
            ] as dom_content_loaded_event_end,
            performance_timing[
                'domContentLoadedEventStart'
            ] as dom_content_loaded_event_start,
            performance_timing['domInteractive'] as dom_interactive,
            performance_timing['domLoading'] as dom_loading,
            performance_timing['domainLookupEnd'] as domain_lookup_end,
            performance_timing['domainLookupStart'] as domain_lookup_start,
            performance_timing['fetchStart'] as fetch_start,
            performance_timing['loadEventEnd'] as load_event_end,
            performance_timing['loadEventStart'] as load_event_start,
            performance_timing['navigationStart'] as navigation_start,
            performance_timing['redirectEnd'] as redirect_end,
            performance_timing['redirectStart'] as redirect_start,
            performance_timing['requestStart'] as request_start,
            performance_timing['responseEnd'] as response_end,
            performance_timing['responseStart'] as response_start,
            performance_timing['secureConnectionStart'] as secure_connection_start,
            performance_timing['unloadEventEnd'] as unload_event_end,
            performance_timing['unloadEventStart'] as unload_event_start,
            event_id as root_id,
            derived_tstamp as root_tstamp
        from contexts
    )

select *
from parsed_timing
