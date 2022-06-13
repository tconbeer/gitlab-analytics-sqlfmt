with
    source as (

        select *
        from {{ source("customers", "customers_db_leads") }}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select

            id::number as leads_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            trial_start_date::timestamp as trial_start_date,
            namespace_id::number as namespace_id,
            user_id::number as user_id,
            opt_in::boolean as opt_in,
            currently_in_trial::boolean as currently_in_trial,
            is_for_business_use::boolean as is_for_business_use,
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            email::varchar as email,
            phone::varchar as phone,
            company_name::varchar as company_name,
            employees_bucket::varchar as employees_bucket,
            country::varchar as country,
            state::varchar as state,
            product_interaction::varchar as product_interaction,
            provider::varchar as provider,
            comment_capture::varchar as comment_capture,
            glm_content::varchar as glm_content,
            glm_source::varchar as glm_source,
            sent_at::timestamp as sent_at,
            website_url::varchar as website_url,
            role::varchar as role,
            jtbd::varchar as jtbd

        from source

    )

select *
from renamed
