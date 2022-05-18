with
    source as (

        select *
        from {{ source("bamboohr", "employment_status") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['id']::number as status_id,
            data_by_row['employeeId']::number as employee_id,
            data_by_row['date']::date as effective_date,
            data_by_row['employmentStatus']::varchar as employment_status,
            nullif(data_by_row['terminationTypeId']::varchar, '') as termination_type
        from intermediate

    ),
    final as (

        select *
        from renamed
        where status_id != 27606  -- incorrect record
        qualify
            row_number() over (
                partition by employee_id, effective_date, employment_status
                order by effective_date
            ) = 1

    )

select *
from final
