with
    audit_results as (

        select sensor_name, time_detected::date as time_detected_date, database_name
        from {{ ref("datasiren_audit_results") }}

    )

select *
from audit_results
