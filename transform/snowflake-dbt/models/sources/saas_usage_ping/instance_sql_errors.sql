with
    base as (select * from {{ source("saas_usage_ping", "instance_sql_errors") }}),
    partitioned as (

        select
            run_id as run_id,
            sql_errors as sql_errors,
            ping_date as ping_date,
            _uploaded_at as uploaded_at
        from base

    ),
    renamed as (

        select
            run_id as run_id,
            try_parse_json(sql_errors) as sql_errors,
            ping_date::timestamp as ping_date,
            dateadd('s', uploaded_at, '1970-01-01')::timestamp as uploaded_at
        from partitioned

    )

select *
from renamed
