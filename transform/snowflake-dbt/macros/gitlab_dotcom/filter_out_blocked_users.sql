{%- macro filter_out_blocked_users(table_to_filter, user_id_column_name) -%}

not
exists (

    select 1
    from {{ ref("gitlab_dotcom_users_source") }} users_source
    where
        users_source.state = 'blocked'
        and users_source.user_id = {{ table_to_filter }}.{{ user_id_column_name }}

)

{%- endmacro -%}
