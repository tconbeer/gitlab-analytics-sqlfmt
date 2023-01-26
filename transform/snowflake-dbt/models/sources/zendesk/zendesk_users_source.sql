with
    source as (select * from {{ source("zendesk", "users") }}),

    renamed as (

        select
            id as user_id,

            -- removed external_id,
            organization_id,

            -- fields
            case when lower(email) like '%gitlab.com%' then name else md5(name) end
            -- masking folks who are submitting tickets! We don't need to surface that.
            as name,
            case when lower(email) like '%gitlab.com%' then email else md5(email) end
            -- masking folks who are submitting tickets! We don't need to surface that.
            as email,
            restricted_agent as is_restricted_agent,
            role,
            suspended as is_suspended,

            -- time
            time_zone,
            created_at,
            updated_at

        from source

    )

select *
from renamed
