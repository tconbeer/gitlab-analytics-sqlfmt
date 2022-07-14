{{ config({"materialized": "incremental", "unique_key": "id"}) }}


with
    source as (

        select *
        from {{ source("version", "version_checks") }}
        {% if is_incremental() %}
        where updated_at >= (select max(updated_at) from {{ this }})
        {% endif %}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as id,
            host_id::number as host_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            gitlab_version::varchar as gitlab_version,
            referer_url::varchar as referer_url,
            parse_json(request_data) as request_data
        from source

    )

select *
from renamed
order by updated_at
