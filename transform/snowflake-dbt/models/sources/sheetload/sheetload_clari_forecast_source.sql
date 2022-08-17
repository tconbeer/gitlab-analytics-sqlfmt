with
    source as (

        select * from {{ source("sheetload", "clari_export_forecast_net_iacv") }}

    ),
    renamed as (

        select
            "User"::varchar as user,
            "Email"::varchar as email,
            "CRM_User_ID"::varchar as crm_user_id,
            "Role"::varchar as sales_team_role,
            "Parent_Role"::varchar as parent_role,
            "Timeframe"::varchar as timeframe,
            "Field"::varchar as field,
            "Week"::number as week,
            "Start_Day"::date as start_date,
            "End_Day"::date as end_date,
            "Data_Type"::varchar as data_type,
            "Data_Value"::varchar as data_value
        from source

    )

select *
from renamed
