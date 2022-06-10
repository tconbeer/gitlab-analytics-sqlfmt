with
    source as (select * from {{ source("sheetload", "headcount") }}),
    renamed as (


        select
            uniquekey::number as primary_key,
            month::date as month_of,
            nullif(function, '') as function,
            try_to_number(employee_cnt) as employee_count
        from source

    )

select *
from renamed
