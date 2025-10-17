with
    source as (

        select *
        from {{ source("bamboohr", "custom_bonus") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['id']::number as bonus_id,
            data_by_row['employeeId']::number as employee_id,
            data_by_row['customBonustype']::varchar as bonus_type,
            data_by_row['customBonusdate']::date as bonus_date,
            data_by_row['customNominatedBy']::varchar as bonus_nominator_type
        from intermediate
        where bonus_date is not null

    )

select *
from renamed
