-- depends on: {{ ref('snowplow_page_views') }}

{{ schema_union_limit('snowplow_', 'snowplow_page_views', 'page_view_start', 90, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
