{{ config({"materialized": "incremental", "unique_key": "id"}) }}

with
    source as (

        select *
        from {{ source("version", "conversational_development_indices") }}
        {% if is_incremental() %}
            where updated_at >= (select max(updated_at) from {{ this }})
        {% endif %}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as id,
            usage_data_id::number as usage_data_id,
            leader_boards::float as leader_boards,
            instance_boards::float as instance_boards,
            leader_ci_pipelines::float as leader_ci_pipelines,
            instance_ci_pipelines::float as instance_ci_pipelines,
            leader_deployments::float as leader_deployments,
            instance_deployments::float as instance_deployments,
            leader_environments::float as leader_environments,
            instance_environments::float as instance_environments,
            leader_issues::float as leader_issues,
            instance_issues::float as instance_issues,
            leader_merge_requests::float as leader_merge_requests,
            instance_merge_requests::float as instance_merge_requests,
            leader_milestones::float as leader_milestones,
            instance_milestones::float as instance_milestones,
            leader_notes::float as leader_notes,
            instance_notes::float as instance_notes,
            leader_projects_prometheus_active::float
            as leader_projects_prometheus_active,
            instance_projects_prometheus_active::float
            as instance_projects_prometheus_active,
            leader_service_desk_issues::float as leader_service_desk_issues,
            instance_service_desk_issues::float as instance_service_desk_issues,
            percentage_boards::float as percentage_boards,
            percentage_ci_pipelines::float as percentage_ci_pipelines,
            percentage_deployments::float as percentage_deployments,
            percentage_environments::float as percentage_environments,
            percentage_issues::float as percentage_issues,
            percentage_merge_requests::float as percentage_merge_requests,
            percentage_milestones::float as percentage_milestones,
            percentage_notes::float as percentage_notes,
            percentage_projects_prometheus_active::float
            as percentage_projects_prometheus_active,
            percentage_service_desk_issues::float as percentage_service_desk_issues,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
