with source as (select * from {{ source("sheetload", "scalable_employment_values") }})

select
    "Month"::date as nbr_month,
    nullif(total_workforce, '')::number as total_workforce,
    nullif(nbr_in_scalable_solution, '')::number as nbr_in_scalable_solution,
    nullif(nbr_in_process, '')::int as nbr_in_process,
    nullif(nbr_to_be_converted, '')::number as nbr_to_be_converted
from source
