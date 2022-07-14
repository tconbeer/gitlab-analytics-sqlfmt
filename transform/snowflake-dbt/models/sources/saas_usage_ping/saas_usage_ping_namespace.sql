{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "saas_usage_ping_gitlab_dotcom_namespace_id",
        }
    )
}}

with
    base as (

        select *
        from {{ source("saas_usage_ping", "namespace") }}
        {% if is_incremental() %}

        where
            dateadd('s', _uploaded_at, '1970-01-01')
            >= (select max(_uploaded_at) from {{ this }})

        {% endif %}
        qualify
            row_number() OVER (
                partition by namespace_ultimate_parent_id, ping_name, ping_date
                order by _uploaded_at desc
            )
            = 1

    ),
    renamed as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["namespace_ultimate_parent_id", "ping_name", "ping_date"]
                )
            }} as saas_usage_ping_gitlab_dotcom_namespace_id,
            namespace_ultimate_parent_id::int as namespace_ultimate_parent_id,
            counter_value::int as counter_value,
            ping_name::varchar as ping_name,
            level::varchar as ping_level,
            query_ran::varchar as query_ran,
            error::varchar as error,
            ping_date::timestamp as ping_date,
            dateadd('s', _uploaded_at, '1970-01-01')::timestamp as _uploaded_at
        from base


    )

select *
from renamed
