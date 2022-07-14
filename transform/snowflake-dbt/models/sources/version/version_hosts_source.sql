{{ config({"materialized": "incremental", "unique_key": "host_id"}) }}

with
    source as (

        select *
        from {{ source("version", "hosts") }}
        {% if is_incremental() %}
        where updated_at >= (select max(updated_at) from {{ this }})
        {% endif %}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as host_id,
            url::varchar as host_url,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            star::boolean as has_star,
            fortune_rank::number as fortune_rank
        from source

    )

select *
from renamed
