with
    dbt_run_results as (select * from {{ ref("dbt_run_results_source") }}),
    dbt_model as (select * from {{ ref("dbt_model_source") }}),
    current_stats as (

        select
            model_unique_id,
            timestampdiff('ms', compilation_started_at, compilation_completed_at)
            / 1000 as compilation_time_seconds_elapsed
        from dbt_run_results
        where compilation_started_at is not null
        qualify
            row_number() over (
                partition by model_unique_id order by compilation_started_at desc
            )
            = 1
        order by 2 desc

    ),
    current_models as (

        select unique_id, name as model_name
        from dbt_model
        where generated_at is not null
        qualify
            row_number() over (partition by unique_id order by generated_at desc) = 1

    ),
    joined as (

        select *
        from current_stats
        inner join
            current_models on current_stats.model_unique_id = current_models.unique_id

    )

select *
from joined
