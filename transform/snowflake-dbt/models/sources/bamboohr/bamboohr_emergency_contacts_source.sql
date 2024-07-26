with
    source as (

        select *
        from {{ source("bamboohr", "emergency_contacts") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['employeeId']::number as employee_id,
            data_by_row['id']::number as emergency_contact_id,
            data_by_row['name']::varchar as full_name,
            data_by_row['homePhone']::varchar as home_phone,
            data_by_row['mobilePhone']::varchar as mobile_phone,
            data_by_row['workPhone']::varchar as work_phone
        from intermediate

    )

select *
from renamed
