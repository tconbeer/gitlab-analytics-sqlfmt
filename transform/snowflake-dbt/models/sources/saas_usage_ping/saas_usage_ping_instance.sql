with
    base as (select * from {{ source("saas_usage_ping", "instance") }}),
    partitioned as (

        select
            query_map as query_map,
            run_results as run_results,
            ping_date as ping_date,
            run_id as run_id,
            recorded_at as recorded_at,
            version as version,
            edition as edition,
            recording_ce_finished_at as recording_ce_finished_at,
            recording_ee_finished_at as recording_ee_finished_at,
            uuid as uuid,
            _uploaded_at as _uploaded_at
        from base
        qualify row_number() OVER (partition by ping_date order by ping_date desc) = 1

    ),
    renamed as (

        select
            {{ dbt_utils.surrogate_key(["ping_date", "run_results"]) }}
            as saas_usage_ping_gitlab_dotcom_id,
            try_parse_json(query_map) as query_map,
            try_parse_json(run_results) as run_results,
            ping_date::timestamp as ping_date,
            run_id as run_id,
            recorded_at::timestamp as recorded_at,
            version as version,
            edition as edition,
            recording_ce_finished_at::timestamp as recording_ce_finished_at,
            recording_ee_finished_at::timestamp as recording_ee_finished_at,
            uuid as uuid,
            dateadd('s', _uploaded_at, '1970-01-01')::timestamp as _uploaded_at
        from partitioned

    )

select *
from renamed
