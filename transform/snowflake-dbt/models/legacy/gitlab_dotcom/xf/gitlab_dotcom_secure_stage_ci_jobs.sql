{{ config({"materialized": "incremental", "unique_key": "ci_build_id"}) }}

with
    ci_builds as (

        select *
        from {{ ref("gitlab_dotcom_ci_builds") }}
        {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

        {% endif %}

    ),
    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    ),
    secure_ci_builds as (

        select
            *,
            case
                when ci_build_name like '%apifuzzer_fuzz%'
                then 'api_fuzzing'
                when ci_build_name like '%container_scanning%'
                then 'container_scanning'
                when ci_build_name like '%dast%'
                then 'dast'
                when ci_build_name like '%dependency_scanning%'
                then 'dependency_scanning'
                when ci_build_name like '%license_management%'
                then 'license_management'
                when ci_build_name like '%license_scanning%'
                then 'license_scanning'
                when ci_build_name like '%sast%'
                then 'sast'
                when ci_build_name like '%secret_detection%'
                then 'secret_detection'
            end as secure_ci_job_type
        from ci_builds
        where
            ci_build_name ilike any (
                '%apifuzzer_fuzz%',
                '%container_scanning%',
                '%dast%',
                '%dependency_scanning%',
                '%license_management%',
                '%license_scanning%',
                '%sast%',
                '%secret_detection%'
            )
    )

    ,
    joined as (

        select
            secure_ci_builds.*,
            namespace_lineage.namespace_is_internal as is_internal_job,
            namespace_lineage.ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id,
            namespace_lineage.ultimate_parent_plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid,

            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_job_creation
        from secure_ci_builds
        left join projects on secure_ci_builds.ci_build_project_id = projects.project_id
        left join
            namespace_lineage on projects.namespace_id = namespace_lineage.namespace_id
        left join
            gitlab_subscriptions
            on namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and secure_ci_builds.created_at
            between gitlab_subscriptions.valid_from and {{
                coalesce_to_infinity(
                    "gitlab_subscriptions.valid_to"
                )
            }}

    )

select *
from secure_ci_builds
