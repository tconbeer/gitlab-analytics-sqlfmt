with
    source as (select * from {{ source("sheetload", "people_budget") }}),
    renamed as (

        select
            "DIVISION"::varchar as division,
            "FISCAL_YEAR"::number as fiscal_year,
            "QUARTER"::number as fiscal_quarter,
            "BUDGET"::number as budget,
            "EXCESS_FROM_PREVIOUS_QUARTER"::number as excess_from_previous_quarter,
            "ANNUAL_COMP_REVIEW"::number as annual_comp_review
        from source

    )

select *
from renamed
