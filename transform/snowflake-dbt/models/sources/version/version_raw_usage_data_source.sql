{{ config({"materialized": "incremental", "unique_key": "raw_usage_data_id"}) }}

with
    source as (

        select *
        from {{ source("version", "raw_usage_data") }}
        {% if is_incremental() %}
        where created_at >= (select max(created_at) from {{ this }})
        {% endif %}
        qualify row_number() over (partition by id order by recorded_at desc) = 1

    ),
    renamed as (

        select
            id::integer as raw_usage_data_id,
            parse_json(payload) as raw_usage_data_payload,
            created_at::timestamp as created_at,
            recorded_at::timestamp as recorded_at
        from source

    )

select *
from renamed
