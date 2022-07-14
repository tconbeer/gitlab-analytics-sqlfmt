with
    source as (select * from {{ source("pto", "gitlab_pto") }}),
    deduped as (

        select *
        from source
        qualify
            row_number() over (
                partition by jsontext['uuid']::varchar order by uploaded_at desc
            )
            = 1

    ),
    each_pto_day as (

        select
            jsontext['end_date']::date as end_date,
            jsontext['start_date']::date as start_date,
            jsontext['status']::varchar as pto_status,
            jsontext['team_member'] ['day_length_hours']::number as employee_day_length,
            jsontext['team_member'] ['department']::varchar as employee_department,
            jsontext['team_member'] ['division']::varchar as employee_division,
            jsontext['team_member'] ['hris_id']::number as hr_employee_id,
            jsontext['team_member'] ['uuid']::varchar as employee_uuid,
            jsontext['uuid']::varchar as pto_uuid,
            ooo_days.value['date']::date as pto_date,
            ooo_days.value['end_time']::timestamp as pto_ends_at,
            ooo_days.value['is_holiday']::boolean as is_holiday,
            ooo_days.value['recorded_hours']::number as recorded_hours,
            ooo_days.value['start_time']::timestamp as pto_starts_at,
            ooo_days.value['total_hours']::number as total_hours
        from deduped, lateral flatten(input => jsontext['ooo_days']::array) ooo_days

    )
select *
from each_pto_day
