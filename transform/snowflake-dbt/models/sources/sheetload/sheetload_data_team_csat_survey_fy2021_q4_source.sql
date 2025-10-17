with
    source as (

        select * from {{ source("sheetload", "data_team_csat_survey_fy2021_q4") }}

    )

select *
from source
