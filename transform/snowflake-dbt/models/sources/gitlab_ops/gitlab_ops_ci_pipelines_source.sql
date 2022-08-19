{{ config({"materialized": "incremental", "unique_key": "ci_pipeline_id"}) }}

with
    source as (

        select *
        from {{ source("gitlab_ops", "ci_pipelines") }}
        where
            created_at is not null

            {% if is_incremental() %}

            and updated_at >= (select max(updated_at) from {{ this }})

            {% endif %}

        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as ci_pipeline_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            ref::varchar as ref,
            tag::boolean as has_tag,
            yaml_errors::varchar as yaml_errors,
            committed_at::timestamp as committed_at,
            project_id::number as project_id,
            status::varchar as status,
            started_at::timestamp as started_at,
            finished_at::timestamp as finished_at,
            duration::number as ci_pipeline_duration,
            user_id::number as user_id,
            lock_version::number as lock_version,
            auto_canceled_by_id::number as auto_canceled_by_id,
            pipeline_schedule_id::number as pipeline_schedule_id,
            source::number as ci_pipeline_source,
            config_source::number as config_source,
            protected::boolean as is_protected,
            failure_reason::varchar as failure_reason,
            iid::number as ci_pipeline_iid,
            merge_request_id::number as merge_request_id
        from source

    )

select *
from renamed
order by updated_at
