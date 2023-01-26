with
    source as (

        select
            id as form_submit_id,
            cookie_id as cookie_id,
            visitor_id as visitor_id,
            session_id as session_id,
            event_date as event_date,
            modified_date as modified_date,
            current_page as current_page,
            current_page_raw as current_page_raw,
            ip_address as ip_address,
            type as type,
            user_agent_string as user_agent_string,
            client_sequence as client_sequence,
            client_random as client_random,
            is_duplicated as is_duplicated,
            is_processed as is_processed,
            email as email,
            form_type as form_type,
            form_source as form_source,
            form_identifier as form_identifier,
            row_key as row_key,
            current_page_key as current_page_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_form_submits") }}

    )

select *
from source
