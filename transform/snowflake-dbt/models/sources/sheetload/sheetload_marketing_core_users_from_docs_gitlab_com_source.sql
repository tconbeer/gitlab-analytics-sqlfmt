with
    source as (

        select *
        from {{ source("sheetload", "marketing_core_users_from_docs_gitlab_com") }}

    ),
    renamed as (

        select
            "Company_Name"::varchar as company_name,
            "Total_Page_Count"::number as total_page_count
        from source
    )

select *
from renamed
