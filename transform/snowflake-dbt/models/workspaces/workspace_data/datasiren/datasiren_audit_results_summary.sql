{{ config({"materialized": "table"}) }}

with
    datasiren_summary as (select * from {{ ref("datasiren_audit_results") }}),
    grouped as (

        select

            sensor_name,
            database_name,
            table_schema,
            table_name,
            column_name,
            count(distinct other_identifier) as rows_detected,
            max(time_detected) as last_detected,
            min(time_detected) as first_detected

        from datasiren_summary {{ dbt_utils.group_by(n=5) }}

    )

select *
from grouped
