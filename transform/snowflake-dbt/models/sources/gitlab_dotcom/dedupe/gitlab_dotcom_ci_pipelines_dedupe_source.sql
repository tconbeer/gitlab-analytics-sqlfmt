{{ config({"materialized": "incremental", "unique_key": "id"}) }}

with
    source as (select * from {{ source("gitlab_dotcom", "ci_pipelines") }}),
    partitioned as (

        select *
        from source

        {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }}) {% endif %}

        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id as id,
            created_at as created_at,
            updated_at as updated_at,
            ref as ref,
            tag as tag,
            yaml_errors as yaml_errors,
            committed_at as committed_at,
            project_id as project_id,
            status as status,
            started_at as started_at,
            finished_at as finished_at,
            duration as duration,
            user_id::number as user_id,
            lock_version as lock_version,
            auto_canceled_by_id as auto_canceled_by_id,
            pipeline_schedule_id as pipeline_schedule_id,
            source as source,
            config_source as config_source,
            protected as protected,
            failure_reason as failure_reason,
            iid as iid,
            merge_request_id::number as merge_request_id,
            _uploaded_at as _uploaded_at
        from partitioned

    )

select *
from renamed
