with
    dbt_source as (

        select *
        from {{ ref("dbt_source_freshness_results_source") }}
        where latest_load_at is not null

    )

select *
from dbt_source
