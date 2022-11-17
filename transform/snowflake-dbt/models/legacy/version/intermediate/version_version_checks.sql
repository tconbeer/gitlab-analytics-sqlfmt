{{ config({"materialized": "table"}) }}

with
    base as (select * from {{ ref("version_version_checks_source") }}),
    final as (

        select
            id,
            host_id,
            created_at,
            updated_at,
            gitlab_version,
            referer_url,
            request_data['HTTP_USER_AGENT']::varchar as http_user_agent,
            request_data['HTTP_REFERER']::varchar as http_referer,
            request_data['HTTP_ACCEPT_LANGUAGE']::varchar as http_accept_language,
            request_data
        from base

    )

select *
from final
