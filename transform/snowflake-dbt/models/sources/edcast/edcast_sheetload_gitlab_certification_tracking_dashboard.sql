with
    source as (

        select *
        from {{ source("sheetload", "gitlab_certification_tracking_dashboard") }}

    ),
    renamed as (

        select
            user::varchar as user,
            account::varchar as account,
            passed_datetime::timestamp as passed_datetime,
            certification::varchar as certification,
            cert_date::date as cert_date,
            partner_sfdc_id::varchar as partner_sfdc_id,
            account_owner::varchar as account_owner,
            region::varchar as region,
            track::varchar as track,
            pubsec_partner::boolean as pubsec_partner,
            cert_month::varchar as cert_month,
            cert_quarter::varchar as cert_quarter,
            dateadd('s', _updated_at, '1970-01-01')::timestamp as _updated_at
        from source

    )

select *
from renamed
