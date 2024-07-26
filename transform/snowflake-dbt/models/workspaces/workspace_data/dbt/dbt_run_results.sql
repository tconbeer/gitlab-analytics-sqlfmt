with
    run_results as (select * from {{ ref("dbt_run_results_source") }}),
    models as (select * from {{ ref("dbt_model_source") }}),
    joined as (

        select
            run_results.model_execution_time,
            run_results.model_unique_id,
            run_results.status as run_status,
            run_results.message as run_message,
            run_results.compilation_started_at,
            run_results.compilation_completed_at,
            run_results.uploaded_at,
            models.name as model_name,
            models.alias as model_alias,
            models.database_name,
            models.schema_name,
            models.package_name,
            models.tags as model_tags,
            models.references as model_references
        from run_results
        inner join models on run_results.model_unique_id = models.unique_id

    )

select *
from joined
