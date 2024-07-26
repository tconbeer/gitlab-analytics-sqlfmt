{%- macro smau_events_ctes(event_name, regexp_where_statements=[]) -%}

    {{ event_name }} as (

        select
            user_snowplow_domain_id,
            user_custom_id,
            to_date(page_view_start) as event_date,
            page_url_path,
            '{{event_name}}' as event_type,
            page_view_id as event_surrogate_key

        from snowplow_page_views
        where
            true
            {% for regexp_where_statement in regexp_where_statements %}
                and page_url_path {{ regexp_where_statement.regexp_function }} '{{regexp_where_statement.regexp_pattern}}'
            {% endfor %}

    )

{%- endmacro -%}
