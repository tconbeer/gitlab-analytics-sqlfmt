{{ config({"materialized": "incremental", "unique_key": "ci_stage_id"}) }}

with
    source as (

        select *
        from {{ source("gitlab_ops", "ci_stages") }}
        where created_at is not null
        qualify
            row_number() OVER (partition by id order by updated_at desc) = 1

            {% if is_incremental() %}

            and updated_at >= (select max(updated_at) from {{ this }}) {% endif %}

    ),
    renamed as (

        select
            id::number as ci_stage_id,
            project_id::number as project_id,
            pipeline_id::number as pipeline_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            name::varchar as ci_stage_name,
            status::number as ci_stage_status,
            lock_version::number as lock_version,
            position::number as position
        from source

    )

select *
from renamed
order by updated_at
