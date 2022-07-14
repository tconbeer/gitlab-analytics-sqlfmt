with
    source as (select * from {{ source("marketo", "lead") }}),
    renamed as (

        select
            -- Primary Key
            id::float as marketo_lead_id,

            -- Info
            email::varchar as email,
            {{ hash_of_column("EMAIL") }}
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            company::varchar as company_name,
            title::varchar as job_title,
            {{ it_job_title_hierarchy("job_title") }},
            country::varchar as country,
            mobile_phone::varchar as mobile_phone,
            inactive_lead_c::boolean as is_lead_inactive,
            inactive_contact_c::boolean as is_contact_inactive,
            sales_segmentation_c::varchar as sales_segmentation,
            is_email_bounced::boolean as is_email_bounced,
            email_bounced_date::date as email_bounced_date,
            unsubscribed::boolean as is_unsubscribed,
            compliance_segment_value::varchar as compliance_segment_value,
            updated_at::timestamp as updated_at

        from source
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    )

select *
from renamed
