{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("mart_usage_event", "mart_usage_event"),
        ]
    )
}},
usage_events as (
    select
        {{ dbt_utils.surrogate_key(["event_date", "event_name", "dim_instance_id"]) }}
        as mart_usage_instance_id,
        event_date,
        event_name,
        data_source,
        dim_instance_id,
        count(*) as event_count,
        count(distinct(dim_user_id)) as distinct_user_count
    from mart_usage_event {{ dbt_utils.group_by(n=5) }}
),
results as (select * from usage_events)


{{
    dbt_audit(
        cte_ref="results",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-02-15",
        updated_date="2022-02-16",
    )
}}
