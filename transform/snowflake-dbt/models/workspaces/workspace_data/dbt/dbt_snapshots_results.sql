with
    snapshots_results as (select * from {{ ref("dbt_snapshots_results_source") }}),
    models as (select * from {{ ref("dbt_model_source") }}),
    joined as (

        select
            snapshots_results.model_execution_time,
            snapshots_results.model_unique_id,
            snapshots_results.status as run_status,
            snapshots_results.message as run_message,
            snapshots_results.compilation_started_at,
            snapshots_results.compilation_completed_at,
            snapshots_results.uploaded_at,
            models.name as model_name,
            models.alias as model_alias,
            models.database_name,
            models.schema_name,
            models.package_name,
            models.tags as model_tags,
            models.references as model_references
        from snapshots_results
        inner join models on snapshots_results.model_unique_id = models.unique_id

    )

select *
from joined
