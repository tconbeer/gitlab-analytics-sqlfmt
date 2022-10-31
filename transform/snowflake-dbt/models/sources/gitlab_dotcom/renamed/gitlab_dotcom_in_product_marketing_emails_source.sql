with
    source as (

        select *
        from {{ ref("gitlab_dotcom_in_product_marketing_emails_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as in_product_marketing_email_id,
            user_id::number as user_id,
            cta_clicked_at::timestamp as cta_clicked_at,
            track::number as track,
            series::number as series,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
