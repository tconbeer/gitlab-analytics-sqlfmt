with
    source as (

        select *
        from {{ source("gitlab_ops", "ci_builds") }}
        qualify row_number() over (partition by id order by _uploaded_at desc) = 1

    ),
    renamed as (

        select
            id::number as ci_build_id,
            status::varchar as status,
            finished_at::timestamp as finished_at,
            trace::varchar as trace,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            started_at::timestamp as started_at,
            runner_id::number as ci_build_runner_id,
            coverage::varchar as coverage,
            commit_id::number as ci_build_commit_id,
            name::varchar as ci_build_name,
            options::varchar as options,
            allow_failure::varchar as allow_failure,
            stage::varchar as stage,
            trigger_request_id::number as ci_build_trigger_request_id,
            stage_idx::number as stage_idx,
            tag::varchar as tag,
            ref::varchar as ref,
            user_id::number as ci_build_user_id,
            type::varchar as type,
            target_url::varchar as target_url,
            description::varchar as description,
            project_id::number as ci_build_project_id,
            erased_by_id::number as ci_build_erased_by_id,
            erased_at::timestamp as ci_build_erased_at,
            artifacts_expire_at::timestamp as ci_build_artifacts_expire_at,
            environment::varchar as environment,
            sha2(yaml_variables::varchar) as yaml_variables,
            queued_at::timestamp as ci_build_queued_at,
            lock_version::varchar as lock_version,
            coverage_regex::varchar as coverage_regex,
            auto_canceled_by_id::number as ci_build_auto_canceled_by_id,
            retried::boolean as retried,
            stage_id::number as ci_build_stage_id,
            protected::boolean as protected,
            failure_reason::varchar as failure_reason,
            scheduled_at::timestamp as ci_build_scheduled_at,
            upstream_pipeline_id::number as upstream_pipeline_id
        from source

    )


select *
from renamed
order by updated_at
